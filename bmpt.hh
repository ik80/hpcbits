/* Copyright 2023 Kaliuzhnyi Ilia

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*/

#pragma once

#include <bits/floatn-common.h>
#include <vector>
#include <unordered_map>
#include <cstdlib>
#include <cstring>

#include <iostream>

template <typename T>
class BitmapTree
{
//#ifndef NDEBUG
    friend int main(int, char *[]);
//#endif

public:
    static const size_t BMP_TREE_HEIGHT = 8;
    static const size_t CHUNK_SIZE = 512;
    static const size_t QWORD_BITS = 64;
    static const size_t CHUNK_SIZE_QWORDS = CHUNK_SIZE / QWORD_BITS;
    // count set bits in chunk
    static size_t chunk_rank(const size_t *bm) noexcept
    {
        const auto pc1 = __builtin_popcountll(bm[0]);
        const auto pc2 = __builtin_popcountll(bm[1]);
        const auto pc3 = __builtin_popcountll(bm[2]);
        const auto pc4 = __builtin_popcountll(bm[3]);
        const auto pc5 = __builtin_popcountll(bm[4]);
        const auto pc6 = __builtin_popcountll(bm[5]);
        const auto pc7 = __builtin_popcountll(bm[6]);
        const auto pc8 = __builtin_popcountll(bm[7]);
        return pc1 + pc2 + pc3 + pc4 + pc5 + pc5 + pc6 + pc7 + pc8;
    }

    // count set bits in chunk up to including pos
    static size_t chunk_rank(const size_t *bm, size_t pos) noexcept
    {
        size_t retval = 0;
        const size_t pos_div = pos/QWORD_BITS;
        const size_t pos_rem = pos%QWORD_BITS;
        switch (pos_div) 
        {
            case 8:
                retval += __builtin_popcountll(bm[7]);
            case 7:
                retval += __builtin_popcountll(bm[6]);
            case 6:
                retval += __builtin_popcountll(bm[5]);
            case 5:
                retval += __builtin_popcountll(bm[4]);
            case 4:
                retval += __builtin_popcountll(bm[3]);
            case 3:
                retval += __builtin_popcountll(bm[2]);
            case 2:
                retval += __builtin_popcountll(bm[1]);
            case 1:
                retval += __builtin_popcountll(bm[0]);
        }
        if (pos_div < 8)
            retval += __builtin_popcountll(bm[pos_div] & ((1ULL << pos_rem) - 1ULL));

        return retval;

    }

    static inline void set_bit(size_t &bitmap, size_t pos)
    {
        bitmap |= (1ULL << pos);
    }
    static inline void clear_bit(size_t &bitmap, size_t pos)
    {
        bitmap &= ~(1ULL << pos);
    }
    static inline bool test_bit(size_t bitmap, size_t pos)
    {
        return bitmap & (1ULL << pos);
    }
    // checks whether bytes pointed to by memory and size are repeated size_t value val
    inline bool memcheck(void *memory, size_t val, size_t size)
    {
        return (*(reinterpret_cast<size_t *>(memory)) == val) && std::memcmp(reinterpret_cast<const char *>(memory), reinterpret_cast<const char *>(memory) + sizeof(size_t), size - sizeof(size_t)) == 0;
    }
    inline std::vector<T> * idx_to_chunk(size_t idx) 
    {
        const size_t bucket_idx = idx / CHUNK_SIZE;
        if (bucket_idx != prev_bucket_idx_)
        {
            prev_bucket_idx_ = bucket_idx;
            prev_chunk_ = &storage[bucket_idx];
        }
        return prev_chunk_;
    }
    BitmapTree(){};
    BitmapTree(size_t capacity) : capacity_(capacity)
    {
        if (capacity_ % CHUNK_SIZE)
            std::abort();
        std::memset(tree_map, 0x0, BMP_TREE_HEIGHT * sizeof(size_t *));
        size_t counter = capacity_;
        size_t prevCounter;
        int levels = 0;
        do
        {
            // init element bitmaps to zeroes
            prevCounter = counter;
            counter /= CHUNK_SIZE;
            tree_map[levels] = new size_t[(counter ? counter : 1) * CHUNK_SIZE_QWORDS]; // bit per element
            std::memset(tree_map[levels], 0x00, (counter ? counter : 1) * CHUNK_SIZE_QWORDS * sizeof(size_t));
            if (!counter)
                top_level_elements_ = prevCounter;
            ++levels;
        } while (counter);

        tree_levels_ = levels;
        // rearrange bitmap tree so root is at the top, easier to think about it this way
        for (size_t i = 0; i < tree_levels_ / 2; ++i)
        {
            size_t *tmp = tree_map[i];
            tree_map[i] = tree_map[tree_levels_ - i - 1];
            tree_map[tree_levels_ - i - 1] = tmp;
        }
    }
    ~BitmapTree()
    {
        for (size_t i = 0; i < tree_levels_; ++i)
            delete[] tree_map[i]; // TODO: leak checks!
    }
    BitmapTree &operator=(const BitmapTree &other) = default; // TODO: tree_map deep copy
    BitmapTree &operator=(BitmapTree &&other) = default;      // TODO: tree_map deep copy
    BitmapTree(const BitmapTree &other) = default;            // TODO: tree_map deep copy
    BitmapTree(BitmapTree &&other) = default;                 // TODO: tree_map deep copy
    void swap(BitmapTree &other);

    // clear bits tree_map upwards from lowest level as needed
    void clear_bits(size_t idx)
    {
        size_t cur_level = tree_levels_ - 1;
        // (idx - idx%CHUNK_SIZE)/QWORD_BITS is the index of the first size_t of the chunk
        clear_bit(tree_map[cur_level][idx / QWORD_BITS], idx % QWORD_BITS);
        bool need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS], 0, CHUNK_SIZE / __CHAR_BIT__);
        // setting the bit
        while (need_to_go_up && cur_level)
        {
            --cur_level;
            idx /= CHUNK_SIZE;
            clear_bit(tree_map[cur_level][idx / QWORD_BITS], idx % QWORD_BITS);
            need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS], 0, CHUNK_SIZE / __CHAR_BIT__);
        }
    }
    // set bits tree_map upwards from lowest level as needed
    void set_bits(size_t idx)
    {
        size_t cur_level = tree_levels_ - 1;
        // (idx - idx%CHUNK_SIZE)/QWORD_BITS is the index of the first size_t of the chunk
        bool need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS], 0, CHUNK_SIZE / __CHAR_BIT__);
        // setting the bit
        set_bit(tree_map[cur_level][idx / QWORD_BITS], idx % QWORD_BITS);
        while (need_to_go_up && cur_level)
        {
            --cur_level;
            idx /= CHUNK_SIZE;
            need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS], 0, CHUNK_SIZE / __CHAR_BIT__);
            set_bit(tree_map[cur_level][idx / QWORD_BITS], idx % QWORD_BITS);
        }
    }
    bool insert(size_t idx, const T &value)
    {
        // test
        if (!test(idx))
        {
            // insert value into storage
            std::vector<T> * chunk = idx_to_chunk(idx);
            const size_t rank = chunk_rank(reinterpret_cast<const size_t *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
            chunk->insert(chunk->begin() + rank, value);
            // set bits tree_map upwards from lowest level
            set_bits(idx);
            return true;
        }
        return false;
    }
    bool emplace(size_t idx, T &&value)
    {
        // test
        if (!test(idx))
        {
            // emplace value into storage
            std::vector<T> * chunk = idx_to_chunk(idx);
            const size_t rank = chunk_rank(reinterpret_cast<const size_t *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
            chunk->emplace(chunk->begin() + rank, value);
            // set bits tree_map upwards from lowest level
            set_bits(idx);
            return true;
        }
        return false;
    }
    bool set(size_t idx, const T &value)
    {
        // test
        // set value into storage
        if (test(idx))
        {
            std::vector<T> * chunk = idx_to_chunk(idx);
            const size_t rank = chunk_rank(reinterpret_cast<const size_t *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
            (*chunk)[rank] = value;
            return true;
        }
        return false;
    }
    bool set(size_t idx, T &&value)
    {
        // test
        // emplace value into storage
        if (test(idx))
        {
            std::vector<T> * chunk = idx_to_chunk(idx);
            const size_t rank = chunk_rank(reinterpret_cast<const size_t *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
            ((*chunk)[rank]) = value;
            return true;
        }
        return false;
    }
    bool erase(size_t idx)
    {
        // test
        if (test(idx))
        {
            // erase value from storage
            std::vector<T> * chunk = idx_to_chunk(idx);
            const size_t rank = chunk_rank(reinterpret_cast<const size_t *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
            chunk->erase(chunk->begin() + rank);
            // clear bits tree_map upwards from lowest level as needed
            clear_bits(idx);
            return true;
        }
        return false;
    }
    bool test(size_t idx)
    {
        return test_bit(tree_map[tree_levels_ - 1][idx / QWORD_BITS], idx % QWORD_BITS);
    }
    bool get(size_t idx, T &value)
    {
        // test
        // return reference to value
        if (test(idx))
        {
            std::vector<T> * chunk = idx_to_chunk(idx);
            const size_t rank = chunk_rank(reinterpret_cast<const size_t *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
            value = (*chunk)[rank];
            return true;
        }
        return false;
    }
    size_t next(size_t idx)
    {
        // overflow check
        if (idx == capacity_ - 1)
            return -1;
        bool existing_element = test(idx);
        //premature optimization is the root of all evil
        if (existing_element && test(idx+1))
            return idx+1;
        // up and then right/down to next set bit in tree_map
        size_t idx_step = 1;
        size_t cur_level = tree_levels_;
        size_t step_hint = 0;
        //while there are no set bits to the right of idx in current chunk - go up one level
        bool need_to_go_up = false;
        do
        {
            --cur_level;
            // check if need to go up. First remainder of current ULL then the rest. 
            const size_t cur_qword = (tree_map[cur_level][idx / QWORD_BITS]) & ((idx % QWORD_BITS) == 63ULL ? 0ULL : (((size_t) (-1)) << (idx % QWORD_BITS + 1ULL)));
            if (!cur_qword) 
            {
                need_to_go_up = true;
                const size_t num_ulls = 7 - (idx % CHUNK_SIZE)/QWORD_BITS;
                for (size_t i = 0; i < num_ulls; ++i) 
                {
                     ++step_hint;
                    if ((tree_map[cur_level][idx / QWORD_BITS + i + 1])) 
                    {
                        need_to_go_up = false;
                        break;
                    }
                }
            }
            else
                need_to_go_up = false;

            if(!need_to_go_up || !cur_level)
                break;
            idx /= CHUNK_SIZE;
            idx_step *= CHUNK_SIZE;
        }
        while(true);
        if (!cur_level && need_to_go_up)
            return -1; // nothing ahead
        //select next bit set after idx
        const size_t cur_qword = (tree_map[cur_level][idx / QWORD_BITS + step_hint]) & (!step_hint ? ((((size_t) (-1)) << (idx % QWORD_BITS + 1))) : (size_t)-1 );
        idx += __builtin_ctzll(cur_qword) - idx % QWORD_BITS + QWORD_BITS*step_hint;
        // go down to lowest level
        while(cur_level != tree_levels_ - 1)
        {
            ++cur_level;
            idx *= CHUNK_SIZE;
            idx_step /= CHUNK_SIZE;
            // select first bit set in chunk
            step_hint = 0;
            const size_t cur_qword = (tree_map[cur_level][idx / QWORD_BITS]);
            if (!cur_qword) 
            {
                const size_t num_ulls = 7;
                for (size_t i = 0; i < num_ulls; ++i) 
                {
                     ++step_hint;
                    if ((tree_map[cur_level][idx / QWORD_BITS + i + 1])) 
                        break;
                }
            }
            idx += __builtin_ctzll(tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS + step_hint]);
        }
        return idx;
    }
    size_t prev(size_t idx)
    {
        // overflow check
        if (idx == 0)
            return -1;
        bool existing_element = test(idx);
        //premature optimization is the root of all evil
        if (existing_element && test(idx-1))
            return idx-1;
        // up and then left/down to next set bit in tree_map
        size_t idx_step = 1;
        size_t cur_level = tree_levels_;
        size_t step_hint = 0;
        //while there are no set bits to the right of idx in current chunk - go up one level
        bool need_to_go_up = false;
        do
        {
            --cur_level;
            // check if need to go up. First remainder of current ULL then the rest. 
            const size_t cur_qword = (tree_map[cur_level][idx / QWORD_BITS]) & ((idx % QWORD_BITS) == 0 ? 0ULL : (((size_t) (-1)) >> (64ULL - idx % QWORD_BITS )));
            if (!cur_qword) 
            {
                need_to_go_up = true;
                const size_t num_ulls = 7 - (CHUNK_SIZE - idx % CHUNK_SIZE)/QWORD_BITS;
                for (size_t i = 0; i < num_ulls; ++i) 
                {
                     ++step_hint;
                    if ((tree_map[cur_level][idx / QWORD_BITS - i - 1])) 
                    {
                        need_to_go_up = false;
                        break;
                    }
                }
            }
            else
                need_to_go_up = false;

            if(!need_to_go_up || !cur_level)
                break;
            idx /= CHUNK_SIZE;
            idx_step *= CHUNK_SIZE;
        }
        while(true);
        if (!cur_level && need_to_go_up)
            return -1; // nothing ahead
        //select next bit set after idx
        const size_t cur_qword = (tree_map[cur_level][idx / QWORD_BITS + step_hint]) & (!step_hint ? ((((size_t) (-1)) >> (64ULL - (idx % QWORD_BITS + 1)))) : (size_t)-1 );
        idx += __builtin_clzll(cur_qword) - idx % QWORD_BITS - QWORD_BITS*step_hint;
        // go down to lowest level
        while(cur_level != tree_levels_ - 1)
        {
            ++cur_level;
            idx *= CHUNK_SIZE;
            idx_step /= CHUNK_SIZE;
            // select first bit set in chunk
            step_hint = 0;
            const size_t cur_qword = (tree_map[cur_level][idx / QWORD_BITS]);
            if (!cur_qword) 
            {
                const size_t num_ulls = 7;
                for (size_t i = 0; i < num_ulls; ++i) 
                {
                     ++step_hint;
                    if ((tree_map[cur_level][idx / QWORD_BITS + i + 1])) 
                        break;
                }
            }
            idx += __builtin_clzll(cur_qword) - idx % QWORD_BITS - QWORD_BITS*step_hint;
        }
        return idx;
    }
    T &get_next(size_t &idx)
    {
        // get(next(idx));
    }
    T &get_prev(size_t &idx)
    {
        // get(prev(idx));
    }
    void clear()
    {
        // clear storage
        // clear tree_map
    }
    void reserve(size_t capacity)
    {
        // reserve storage
        // reallocate tree_map levels accordingly
    }

private:
    // tree bit map
    size_t *tree_map[BMP_TREE_HEIGHT] = {0}; // TODO: std::vector<std::vector<size_t>>, starts at bottom level
    // idx/CHUNK_SIZE -> chunked element storage
    std::unordered_map<size_t, std::vector<T>> storage;
    size_t tree_levels_ = 0;
    size_t capacity_ = 0;
    size_t top_level_elements_ = 0;
    size_t size_ = 0;
    size_t prev_bucket_idx_ = (size_t)-1;
    std::vector<T> * prev_chunk_ = nullptr;

};

template <typename T>
void swap(BitmapTree<T> &lhs, BitmapTree<T> &rhs)
{
    lhs.swap(rhs);
}

// TODO: all this is supposed to be used as primary key/autoincrement value in a database table: bmpt<size_t/*unique*/, Record>, or {bmpt<size_t/*unique*/, Field1>, ... , bmpt<size_t/*unique*/, FieldN>} if columnar
// TODO: in this case, foreign key would be bmpt<size_t/*primary key of foreign table*/, size_t/*primary key of this table> and joins would be just AND of bmpt bit structure on all levels, making it a BITMAP JOIN 
// TODO: autoincrement value would just be found by looking any zero in bmpt bitmap structure, also in NLog512(N) time, keeping it always < size of the table
// TODO: excessive memory usage for bitmap can be actually done using same approach as values - hashmaps to bitmap pieces on all levels of bitmap tree
// TODO: to use thread_local cached chunks, LF hash of tid->idx is needed in the class instance
// TODO: to get iterator stability, change storage from vector to deque + indexing/slots array
