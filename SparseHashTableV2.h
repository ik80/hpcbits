#pragma once

#include <functional>
#include <atomic>
#include <cstdlib>
#include <climits>
#include <utility>
#include <memory>
#include <array>
#include <vector>
#include <deque>
#include <algorithm>

template <
    typename K,
    typename V,
    typename HashFunc = std::hash<K>, // uniform hash function is needed, xxhash etc
    typename KeyEqual = std::equal_to<K>,
    typename Allocator = std::allocator<std::pair<const K, V>>>
struct CantStopHashMap
{
    typedef K key_type;
    typedef V mapped_type;
    typedef std::pair<const K, V> value_type;
    typedef HashFunc hasher; // uniform hash function is needed, xxhash etc
    typedef std::ptrdiff_t difference_type;
    typedef value_type *pointer;
    typedef const value_type *const_pointer;
    typedef value_type &reference;
    typedef const value_type &const_reference;

    static const size_t SLOTS_PER_BUCKET = 80 * CHAR_BIT; // 640
    static const size_t MAX_ELEMENTS_PER_BUCKET = 256;    // 2^CHAR_BIT for slots
    static const size_t TARGET_ELEMENTS_PER_BUCKET = 128; // num_buckets = max_elements / TARGET_ELEMENTS_PER_BUCKET

    // TODO: if you use "deque" for elements and keep vector of deleted elements, iterators can be made stable

    struct CantStopHashMapBucket // two cachelines
    {
        // TODO: 1 cacheline structure
        // static const size_t SLOTS_PER_BUCKET = 50 * CHAR_BIT; // 400
        // static const size_t MAX_ELEMENTS_PER_BUCKET = 256;    // 2^CHAR_BIT for slots
        // static const size_t TARGET_ELEMENTS_PER_BUCKET = 64; // num_buckets = max_elements / TARGET_ELEMENTS_PER_BUCKET
        // value_type * elements : 48;
        // unsigned char * slots : 48;
        // unsigned char capacity : 8;
        // unsigned char size : 8;
        // unsigned char bitmap[SLOTS_PER_BUCKET / CHAR_BIT] = {0}; 

        std::vector<value_type> elements;                        // 24
        //std::deque<value_type> elements;                       // 80 use this to get iterator stability
        //std::vector<unsigned char> free_slots;                 // 24
        std::vector<unsigned char> slots;                        // 24
        unsigned char bitmap[SLOTS_PER_BUCKET / CHAR_BIT] = {0}; // 80

        inline bool bitmap_test(size_t offset)
        {
            return bitmap[offset / CHAR_BIT] & (1 << (offset % CHAR_BIT));
        }

        inline void bitmap_set(size_t offset)
        {
            bitmap[offset / CHAR_BIT] |= (1 << (offset % CHAR_BIT));
        }

        inline void bitmap_clear(size_t offset)
        {
            bitmap[offset / CHAR_BIT] &= ~(1 << (offset % CHAR_BIT));
        }

        // TODO: intrinsics for MSVC/GCC/CLANG here 256 bit -> 128 bit -> 64 bit -> 64 bit masked
        inline size_t rank_at_pos_lookup(size_t pos)
        {
            size_t retval = 0;
            const size_t pos_copy = pos;
            static constexpr char bits_in_char[256] =
            {
                0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
                4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
            };
            const unsigned char *bm = (const unsigned char *)bitmap;
            for (; pos > 8ULL; pos -= 8ULL)
                retval += bits_in_char[*bm++];
            const auto res = retval + bits_in_char[*bm & ((1ULL << pos) - 1ULL)];
            //const auto res1 = rank_at_pos(pos_copy);
            //if (res != res1)
            //    abort();
            return res;
        }

        inline size_t rank_at_pos(size_t pos)
        {
            size_t retval = 0;
            const unsigned long long * bm = (const unsigned long long *) bitmap;
            for (; pos >= 64ULL; pos -= 64ULL)
                retval += __builtin_popcountll(*bm++);
            return retval + (pos ? __builtin_popcountll(*bm & ((1ULL << pos) - 1ULL)) : 0);
        }


    };

    std::vector<CantStopHashMapBucket> buckets;   // 24
    size_t max_elements = 0;                      // 8
    size_t num_elements = 0;                      // 8
    size_t num_buckets = 0;                       // 8

    explicit CantStopHashMap(size_t in_max_elements);
    bool insert(const K &key, const V &value);
    bool put(const K &key, const V &value); // inserted(false) or set(true)
    bool get(const K &key, V &value);
    bool set(const K &key, const V &value);
    bool remove(const K &key);
};

template <typename K, typename V, typename HashFunc, typename KeyEqual, typename Allocator>
CantStopHashMap<K, V, HashFunc, KeyEqual, Allocator>::CantStopHashMap(size_t in_max_elements)
{
    num_buckets = (in_max_elements / (TARGET_ELEMENTS_PER_BUCKET)) + 1;
    max_elements = num_buckets * SLOTS_PER_BUCKET;
    buckets.resize(num_buckets);
}

template <typename K, typename V, typename HashFunc, typename KeyEqual, typename Allocator>
bool CantStopHashMap<K, V, HashFunc, KeyEqual, Allocator>::insert(const K &key, const V &value)
{
    const size_t key_hash = HashFunc()(key);
    size_t idx = key_hash % max_elements;
    size_t bucket_idx = idx / SLOTS_PER_BUCKET;
    size_t bucket_offset = idx % SLOTS_PER_BUCKET;
    size_t rank = buckets[bucket_idx].rank_at_pos(bucket_offset + 1);

    if (buckets[bucket_idx].elements.empty())
    {
        buckets[bucket_idx].bitmap_set(bucket_offset);
        buckets[bucket_idx].slots.push_back(0);
        buckets[bucket_idx].elements.emplace_back(key, value);
        return true;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        size_t rank = buckets[bucket_idx].rank_at_pos(bucket_offset + 1);
        while (true) // linear probing
        {
            bool elementExists = buckets[bucket_idx].bitmap_test(bucket_offset);
            if (elementExists)
            {
                if (buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].first == key)
                    return false;
                else
                {
                    stepBack = true;
                    ++rank;
                    ++bucket_offset;
                    idx = (idx + 1) % max_elements;
                    if (bucket_offset == SLOTS_PER_BUCKET || !idx)
                    {
                        rank = 1;
                        bucket_idx = idx / SLOTS_PER_BUCKET;
                        bucket_offset = idx % SLOTS_PER_BUCKET;
                        if (buckets[bucket_idx].elements.empty())
                        {
                            buckets[bucket_idx].bitmap_set(bucket_offset);
                            buckets[bucket_idx].slots.push_back(0);
                            buckets[bucket_idx].elements.emplace_back(key, value);
                            return true;
                        }
                    }
                }
            }
            else
            {
                if (stepBack)
                    --rank;
                size_t count = buckets[bucket_idx].elements.size();
                unsigned char new_slot = buckets[bucket_idx].elements.size();
                buckets[bucket_idx].slots.insert(buckets[bucket_idx].slots.begin()+rank, new_slot);
                buckets[bucket_idx].elements.emplace_back(key, value);
                buckets[bucket_idx].bitmap_set(bucket_offset);
                return true;
            }
        }
    }
}

template <typename K, typename V, typename HashFunc, typename KeyEqual, typename Allocator>
bool CantStopHashMap<K, V, HashFunc, KeyEqual, Allocator>::put(const K &key, const V &value)
{
    const size_t key_hash = HashFunc()(key);
    size_t idx = key_hash % max_elements;
    size_t bucket_idx = idx / SLOTS_PER_BUCKET;
    size_t bucket_offset = idx % SLOTS_PER_BUCKET;
    size_t rank = buckets[bucket_idx].rank_at_pos(bucket_offset + 1);

    if (buckets[bucket_idx].elements.empty())
    {
        buckets[bucket_idx].bitmap_set(bucket_offset);
        buckets[bucket_idx].slots.push_back(0);
        buckets[bucket_idx].elements.emplace_back(key, value);
        return false;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        size_t rank = buckets[bucket_idx].rank_at_pos(bucket_offset + 1);
        while (true) // linear probing
        {
            bool elementExists = buckets[bucket_idx].bitmap_test(bucket_offset);
            if (elementExists)
            {
                if (buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].first == key)
                {
                    buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].second = value;
                    return true;
                }
                else
                {
                    stepBack = true;
                    ++rank;
                    ++bucket_offset;
                    idx = (idx + 1) % max_elements;
                    if (bucket_offset == SLOTS_PER_BUCKET || !idx)
                    {
                        rank = 1;
                        bucket_idx = idx / SLOTS_PER_BUCKET;
                        bucket_offset = idx % SLOTS_PER_BUCKET;
                        if (buckets[bucket_idx].elements.empty())
                        {
                            buckets[bucket_idx].bitmap_set(bucket_offset);
                            buckets[bucket_idx].slots.push_back(0);
                            buckets[bucket_idx].elements.emplace_back(key, value);
                            return false;
                        }
                    }
                }
            }
            else
            {
                if (stepBack)
                    --rank;
                size_t count = buckets[bucket_idx].elements.size();;
                unsigned char new_slot = buckets[bucket_idx].elements.size();
                buckets[bucket_idx].slots.insert(buckets[bucket_idx].slots.begin()+rank, new_slot);
                buckets[bucket_idx].elements.emplace_back(key, value);
                buckets[bucket_idx].bitmap_set(bucket_offset);
                return false;
            }
        }
    }
}

template <typename K, typename V, typename HashFunc, typename KeyEqual, typename Allocator>
bool CantStopHashMap<K, V, HashFunc, KeyEqual, Allocator>::get(const K &key, V &value)
{
    const size_t key_hash = HashFunc()(key);
    size_t idx = key_hash % max_elements;
    size_t bucket_idx = idx / SLOTS_PER_BUCKET;
    size_t bucket_offset = idx % SLOTS_PER_BUCKET;
    size_t rank = buckets[bucket_idx].rank_at_pos(bucket_offset + 1);

    while (buckets[bucket_idx].bitmap_test(bucket_offset)) // linear probing
    {
        if (buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].first == key)
        {
            value = buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].second; // read value
            return true;
        }
        else
        {
            ++rank;
            ++bucket_offset;
            idx = (idx + 1) % max_elements;
            if (bucket_offset == SLOTS_PER_BUCKET || !idx)
            {
                rank = 1;
                bucket_idx = idx / SLOTS_PER_BUCKET;
                bucket_offset = idx % SLOTS_PER_BUCKET;
            }
        }
    }
    return false;
}

template <typename K, typename V, typename HashFunc, typename KeyEqual, typename Allocator>
bool CantStopHashMap<K, V, HashFunc, KeyEqual, Allocator>::set(const K &key, const V &value)
{
    const size_t key_hash = HashFunc()(key);
    size_t idx = key_hash % max_elements;
    size_t bucket_idx = idx / SLOTS_PER_BUCKET;
    size_t bucket_offset = idx % SLOTS_PER_BUCKET;
    size_t rank = buckets[bucket_idx].rank_at_pos(bucket_offset + 1);

    while (buckets[bucket_idx].bitmap_test(bucket_offset)) // linear probing
    {
        if (buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].first == key)
        {
            buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].second = value;
            return true;
        }
        else
        {
            ++rank;
            ++bucket_offset;
            idx = (idx + 1) % max_elements;
            if (bucket_offset == SLOTS_PER_BUCKET || !idx)
            {
                rank = 1;
                bucket_idx = idx / SLOTS_PER_BUCKET;
                bucket_offset = idx % SLOTS_PER_BUCKET;
            }
        }
    }
    return false;
}

template <typename K, typename V, typename HashFunc, typename KeyEqual, typename Allocator>
bool CantStopHashMap<K, V, HashFunc, KeyEqual, Allocator>::remove(const K &key)
{
    const size_t key_hash = HashFunc()(key);
    size_t idx = key_hash % max_elements;
    size_t bucket_idx = idx / SLOTS_PER_BUCKET;
    size_t bucket_offset = idx % SLOTS_PER_BUCKET;
    bool element_found = false;
    // first, find the matching record
    size_t rank;
    while (buckets[bucket_idx].bitmap_test(bucket_offset))
    {
        if (!buckets[bucket_idx].elements.empty())
            rank = buckets[bucket_idx].rank_at_pos(bucket_offset + 1);
        if (!buckets[bucket_idx].elements.empty() && buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].first == key)
        {
            element_found = true;
            break;
        }
        ++bucket_offset;
        idx = (idx + 1) % max_elements;
        if (bucket_offset == SLOTS_PER_BUCKET || !idx)
        {
            bucket_idx = idx / SLOTS_PER_BUCKET;
            bucket_offset = idx % SLOTS_PER_BUCKET;
        }
    }
    if (!element_found)
        return false;

    // walk forward until next hole to see if any records need to be moved back
    size_t deleted_idx = idx, original_deleted_idx = deleted_idx;
    ++bucket_offset;
    idx = (idx + 1) % max_elements;
    if (bucket_offset == SLOTS_PER_BUCKET || !idx)
    {
        rank = 0; // if there are window elements rank will be incremented later
        bucket_idx = idx / SLOTS_PER_BUCKET;
        bucket_offset = idx % SLOTS_PER_BUCKET;
    }
    while (!buckets[bucket_idx].elements.empty() && (buckets[bucket_idx].bitmap_test(bucket_offset)))
    {
        // calc hash. If its less or equal new hole position, swap, save new hole and move on
        ++rank;
        if (!buckets[bucket_idx].elements.empty())
        {
            size_t idx_at_rank = HashFunc()(buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]].first) % max_elements;
            if ((idx_at_rank <= deleted_idx && deleted_idx - idx_at_rank < max_elements / 2) || (idx_at_rank > deleted_idx && idx_at_rank - deleted_idx >= max_elements / 2))// TODO: this if doesnt work if mismatch is larger than half of the table
            {
                // swap
                size_t swap_pos = deleted_idx / SLOTS_PER_BUCKET;
                size_t swap_offset = deleted_idx % SLOTS_PER_BUCKET;
                size_t swap_rank = buckets[swap_pos].rank_at_pos(swap_offset + 1);
                std::swap((std::pair<K,V>&)buckets[swap_pos].elements[buckets[swap_pos].slots[swap_rank - 1]], (std::pair<K,V>&)buckets[bucket_idx].elements[buckets[bucket_idx].slots[rank - 1]]);
                deleted_idx = idx;
            }
        }
        ++bucket_offset;
        idx = (idx + 1) % max_elements;
        if (bucket_offset == SLOTS_PER_BUCKET || !idx)
        {
            rank = 0; // if there are window elements rank will be incremented later
            bucket_idx = idx / SLOTS_PER_BUCKET;
            bucket_offset = idx % SLOTS_PER_BUCKET;
        }
    }

    // remove record
    size_t deleted_bucket_idx = deleted_idx / SLOTS_PER_BUCKET;
    size_t deleted_bucket_offset = deleted_idx % SLOTS_PER_BUCKET;
    size_t deleted_count = buckets[deleted_bucket_idx].elements.size();;

    if (deleted_count == 1)
    {
        buckets[deleted_bucket_idx].elements.clear();    
        buckets[deleted_bucket_idx].slots.clear();    
    }
    else
    {
        size_t deleted_rank = buckets[deleted_bucket_idx].rank_at_pos(deleted_bucket_offset + 1);
        if (deleted_rank < deleted_count) 
        {
            // move last element to deleted position, update slots
            // save deleted slot
            size_t deleted_slot = buckets[deleted_bucket_idx].slots[deleted_rank - 1];
            // find slot position for last element
            auto last_slot_iter = std::find(buckets[deleted_bucket_idx].slots.begin(), buckets[deleted_bucket_idx].slots.end(), buckets[deleted_bucket_idx].slots.size()-1);
            // move element
            (std::pair<K,V>&)buckets[deleted_bucket_idx].elements[deleted_slot] = std::move((std::pair<K,V>&)buckets[deleted_bucket_idx].elements[buckets[deleted_bucket_idx].elements.size()-1]);
            // update slots according to move
            *last_slot_iter = deleted_slot;
        }
        buckets[deleted_bucket_idx].slots.erase(buckets[deleted_bucket_idx].slots.begin() + deleted_rank - 1);
    }
    buckets[deleted_bucket_idx].bitmap_clear(deleted_bucket_offset);
    return true;
}
