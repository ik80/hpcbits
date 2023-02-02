#pragma once

#include <functional>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <stdexcept>
#include <utility>
#include <memory>

// TODO: implement save/load/rehash in a separate friend class\file LFSparseHashTableUtil
// TODO: Implement support for monotonous keys for given key - truncate last bits for number of pairs to fit into cache line - hash, add truncated bits
template<typename K, typename V, class HashFunc = std::hash<K> >
class LFSparseHashTableSimpleV2
{
public:
    typedef K key_type;
    typedef V mapped_type;
    typedef std::pair<const K, V> value_type;
    typedef HashFunc hasher;
    typedef std::ptrdiff_t difference_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;

    struct SparseBucket
    {
        static const unsigned long long ELEMENTS_PER_BUCKET = 16ULL;
        static const unsigned long long POINTER_BITS = 47ULL;
        static const unsigned long long LOCK_BITS = 1ULL;

        unsigned long long data = 0; 

        inline std::pair<const K, V>* getElements() const 
        {
            return (std::pair<const K, V>*) (data & 0x0000FFFFFFFFFFFEULL);
        }

        inline void setElements(std::pair<const K, V>* ptr) 
        {
            data = (data & 0xFFFF000000000001ULL) | ((unsigned long long)ptr & 0x0000FFFFFFFFFFFEULL);
        }
        inline void lockBucket()
        {
            unsigned long long newPointer, expectedPointer = data;
            while (true)
            {
                expectedPointer &= 0xFFFFFFFFFFFFFFFEULL; // expect last bit to be zero
                newPointer = expectedPointer | 1ULL;
                if (std::atomic_compare_exchange_strong((std::atomic_ullong*)&data, &expectedPointer, newPointer))
                    break;
            }
        }
        inline void unlockBucket()
        {
            unsigned long long newPointer, oldPointer = data, expectedPointer;
            expectedPointer = oldPointer | 1ULL; // expect last bit to be one
            newPointer = expectedPointer & 0xFFFFFFFFFFFFFFFEULL;
            std::atomic_compare_exchange_strong((std::atomic_ullong*)&data, &expectedPointer, newPointer);
        }
        inline void bitmapSet(unsigned long long pos)
        {
            data |= (1ULL << (pos + POINTER_BITS + LOCK_BITS));
        }
        inline void bitmapClear(unsigned long long pos)
        {
            data &= ~(1ULL << (pos + POINTER_BITS + LOCK_BITS));
        }
        inline bool bitmapTest(unsigned long long pos)
        {
            return data & (1ULL << (pos + POINTER_BITS + LOCK_BITS));
        }
        inline unsigned long long rankAtPos(unsigned long long pos) 
        {
            // TODO: intrinsics for MSVC/GCC/CLANG
            unsigned long long retval = 0;
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
            const unsigned char* bm = (const unsigned char*)&data + (POINTER_BITS+LOCK_BITS) / CHAR_BIT;
            for (; pos > 8ULL; pos -= 8ULL)
                retval += bits_in_char[*bm++];
            return retval + bits_in_char[*bm & ((1ULL << pos) - 1ULL)];
        }
    };

private:

    char padding0[64]; // padding to cacheline
    SparseBucket* buckets; //8
    unsigned long long maxElements; //8
    HashFunc hasherFunc; // padded to 4
    char padding1[44]; // padding to cacheline

public:
    LFSparseHashTableSimpleV2(unsigned long long inMaxElements = 1024, const HashFunc & inHasher = HashFunc());
    ~LFSparseHashTableSimpleV2();
    LFSparseHashTableSimpleV2(const LFSparseHashTableSimpleV2& other);
    LFSparseHashTableSimpleV2& operator= (const LFSparseHashTableSimpleV2& other);

    // below operations are wait-free
    // IMPORTANT: additional flags for set and get allows for implementation of CAS, FETCH_ADD, FETCH_SUB etc. in client code.
    // Example: if (get(false)) { ++value; set ( true ) } is equivalent to FETCH_ADD
    // Example: if (get(false) && (value==expected)) set ( true ); is equivalent to CAS
    bool insert(const K & inKey, const V & inValue);
    bool insertOrSet(const K & inKey, const V & inValue);
    bool get(const K & inKey, V & inValue, bool unlock = true); // NOTE: if called with unlock == false, it will try to get the element and leave it locked if found
    bool set(const K & inKey, const V & inValue, bool locked = false); // NOTE: if called with locked == true, it will not try to lock element but will unlock it after set
    bool remove(const K & inKey);
    void unlockBuckets(unsigned long long startBucketIdx, unsigned long long endBucketIdx);

    void swap(LFSparseHashTableSimpleV2& other);
};

template<typename K, typename V, class HashFunc>
void LFSparseHashTableSimpleV2<K, V, HashFunc>::unlockBuckets(unsigned long long startBucketIdx, unsigned long long endBucketIdx)
{

    if (startBucketIdx == endBucketIdx)
        buckets[startBucketIdx].unlockBucket();
    else if (endBucketIdx > startBucketIdx)
    {
        while (endBucketIdx > startBucketIdx) { buckets[endBucketIdx--].unlockBucket(); };
        buckets[startBucketIdx].unlockBucket();
    }
    else // wrapped the bitch
    {
        unsigned long long tmp = 0;
        while (endBucketIdx > tmp) { buckets[endBucketIdx--].unlockBucket(); };
        buckets[0].unlockBucket();

        tmp = maxElements / SparseBucket::ELEMENTS_PER_BUCKET + (maxElements % SparseBucket::ELEMENTS_PER_BUCKET ? 1ULL : 0ULL);
        while(tmp > startBucketIdx)
        {
            buckets[--tmp].unlockBucket();
        };
    }
}

template<typename K, typename V, class HashFunc>
void LFSparseHashTableSimpleV2<K, V, HashFunc>::swap(LFSparseHashTableSimpleV2<K, V, HashFunc>& other)
{
    std::swap(hasherFunc, other.hasherFunc);
    std::swap(buckets, other.buckets);
    std::swap(maxElements, other.maxElements);
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimpleV2<K, V, HashFunc>::LFSparseHashTableSimpleV2(unsigned long long inMaxElements, const HashFunc & inHasher) :
    buckets(0), maxElements(inMaxElements), hasherFunc(inHasher)
{
    buckets = (SparseBucket*)std::malloc((maxElements / SparseBucket::ELEMENTS_PER_BUCKET + 1) * sizeof(SparseBucket));
    memset(buckets, 0, (maxElements / SparseBucket::ELEMENTS_PER_BUCKET + 1) * sizeof(SparseBucket));
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimpleV2<K, V, HashFunc>::~LFSparseHashTableSimpleV2()
{
    for (unsigned long long bucketPos = 0; bucketPos < maxElements / SparseBucket::ELEMENTS_PER_BUCKET + 1; ++bucketPos)
    {
        SparseBucket * bucket = &(buckets[bucketPos]);
        unsigned long long count = bucket->rankAtPos(SparseBucket::ELEMENTS_PER_BUCKET);
        std::pair<const K, V> * bucketElements = bucket->getElements(); // destructor should work when no buckets are locked
        for (unsigned long long j = 0; j < count; ++j)
        {
            bucketElements[j].~pair();
        }
        std::free(bucket->getElements());
    }
    std::free(buckets);
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimpleV2<K, V, HashFunc>::LFSparseHashTableSimpleV2(const LFSparseHashTableSimpleV2& other) :
    buckets(0), maxElements(other.maxElements), hasherFunc(other.hasherFunc)
{
    buckets = (SparseBucket*)std::malloc((maxElements / SparseBucket::ELEMENTS_PER_BUCKET + 1) * sizeof(SparseBucket));
    memset(buckets, 0, (maxElements / SparseBucket::ELEMENTS_PER_BUCKET + 1) * sizeof(SparseBucket));
    for (unsigned long long bucketPos = 0; bucketPos < other.maxElements / SparseBucket::ELEMENTS_PER_BUCKET + 1; ++bucketPos)
    {
        buckets[bucketPos] = other.buckets[bucketPos];
        unsigned long long count = buckets[bucketPos].rankAtPos(SparseBucket::ELEMENTS_PER_BUCKET);
        buckets[bucketPos].setElements((std::pair<const K, V>*) std::malloc(count * sizeof(std::pair<const K, V>))); // all buckets are created unlocked
        memcpy(buckets[bucketPos].getElements(), other.buckets[bucketPos].getElements(), count * sizeof(std::pair<const K, V>)); // other table buckets should be unlocked
    }
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimpleV2<K, V, HashFunc>& LFSparseHashTableSimpleV2<K, V, HashFunc>::operator= (const LFSparseHashTableSimpleV2& other)
{
    if (&other != this)
    {
        LFSparseHashTableSimpleV2<K, V, HashFunc> tmpTable(other);
        swap(tmpTable);
    }
    return *this;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimpleV2<K, V, HashFunc>::insertOrSet(const K & inKey, const V & inValue)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<const K, V> * bucketElements = bucket->getElements();
    if (bucketElements == nullptr)
    {
        bucket->setElements((std::pair<const K, V> *)std::malloc(sizeof(std::pair<const K, V>)));
        bucketElements = bucket->getElements();
        new (&(bucketElements[0])) std::pair<const K, V>(inKey, inValue);
        bucket->bitmapSet(bucketOffset);
        unlockBuckets(startBucketIdx, endBucketIdx);
        return false;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        unsigned long long rank = bucket->rankAtPos(bucketOffset + 1);
        while (true) // linear probing
        {
            bool elementExists = bucket->bitmapTest(bucketOffset);
            if (elementExists)
            {
                if (bucketElements[rank - 1].first == inKey)
                {
                    bucketElements[rank - 1].second = inValue;
                    unlockBuckets(startBucketIdx, endBucketIdx);
                    return true;
                }
                else
                {
                    stepBack = true;
                    ++rank;
                    ++bucketOffset;
                    idx = (idx + 1) % maxElements;
                    if (bucketOffset == SparseBucket::ELEMENTS_PER_BUCKET || !idx)
                    {
                        rank = 1;
                        bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
                        bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
                        bucket = &(buckets[bucketPos]);
                        if (bucketPos != startBucketIdx)
                            bucket->lockBucket();
                        endBucketIdx = bucketPos;
                        bucketElements = bucket->getElements();
                        if (bucketElements == nullptr)
                        {
                            bucketElements = (std::pair<const K, V> *) std::malloc(sizeof(std::pair<const K, V>));
                            bucket->setElements(bucketElements);
                            bucketElements = bucket->getElements();
                            new (&(bucketElements[0])) std::pair<const K, V>(inKey, inValue);
                            bucket->bitmapSet(bucketOffset);
                            unlockBuckets(startBucketIdx, endBucketIdx);
                            return false;
                        }
                    }
                }
            }
            else
            {
                if (stepBack)
                    --rank;
                unsigned long long count = bucket->rankAtPos(SparseBucket::ELEMENTS_PER_BUCKET);
                bucketElements = (std::pair<const K, V> *) std::realloc(bucketElements, (count + 1) * sizeof(std::pair<const K, V>));
                bucket->setElements(bucketElements);
                bucketElements = bucket->getElements();
                if (rank < count)
                    memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(std::pair<const K, V>));
                new (&(bucketElements[rank])) std::pair<const K, V>(inKey, inValue);
                bucket->bitmapSet(bucketOffset);
                unlockBuckets(startBucketIdx, endBucketIdx);
                return false;
            }
        }
    }
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimpleV2<K, V, HashFunc>::get(const K & inKey, V & value, bool unlock)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long lockRangeStart = bucketPos;
    unsigned long long lockRangeEnd = bucketPos;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<const K, V> * bucketElements = bucket->getElements();
    bucketElements = (std::pair<const K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    unsigned long long rank = bucket->rankAtPos(bucketOffset + 1);
    while (bucket->bitmapTest(bucketOffset)) // linear probing
    {
        if (bucketElements && bucketElements[rank - 1].first == inKey)
        {
            value = bucketElements[rank - 1].second; // read value
            if (unlock)
            {
                if (lockRangeStart == lockRangeEnd) // unlock element
                {
                    bucket->unlockBucket();
                }
                else // or element range is there were collisions
                {
                    unlockBuckets(lockRangeStart, lockRangeEnd);
                }
            }
            return true;
        }
        else
        {
            ++rank;
            ++bucketOffset;
            idx = (idx + 1) % maxElements;
            if (bucketOffset == SparseBucket::ELEMENTS_PER_BUCKET || !idx)
            {
                rank = 1;
                bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
                lockRangeEnd = bucketPos;
                bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
                bucket = &(buckets[bucketPos]);
                if (bucketPos != lockRangeStart)
                    bucket->lockBucket();
                bucketElements = bucket->getElements();
            }
        }
    }
    if (unlock)
    {
        if (lockRangeStart == lockRangeEnd) // unlock element
        {
            bucket->unlockBucket();
        }
        else // or element range is there were collisions
        {
            unlockBuckets(lockRangeStart, lockRangeEnd);
        }
    }
    return false;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimpleV2<K, V, HashFunc>::remove(const K & inKey)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<const K, V> * bucketElements = bucket->getElements();
    bool elementFound = false;

    // first, find the matching record
    unsigned long long rank;
    while (bucket->bitmapTest(bucketOffset))
    {
        if (bucketElements)
            rank = bucket->rankAtPos(bucketOffset + 1);
        if (bucketElements && bucketElements[rank - 1].first == inKey)
        {
            elementFound = true;
            break;
        }
        ++bucketOffset;
        idx = (idx + 1) % maxElements;
        if (bucketOffset == SparseBucket::ELEMENTS_PER_BUCKET || !idx)
        {
            bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
            bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
            bucket = &(buckets[bucketPos]);
            if (bucketPos != startBucketIdx)
                bucket->lockBucket();
            endBucketIdx = bucketPos;
            bucketElements = bucket->getElements();
        }
    }
    if (!elementFound)
    {
        unlockBuckets(startBucketIdx, endBucketIdx);
        return false;
    }
    else
    {
        bucketElements[rank - 1].~pair(); // destroy the deleted element
    }

    // walk forward until next hole to see if any records need to be moved back
    unsigned long long deletedIdx = idx, originalDeletedIdx = deletedIdx;
    ++bucketOffset;
    idx = (idx + 1) % maxElements;
    if (bucketOffset == SparseBucket::ELEMENTS_PER_BUCKET || !idx)
    {
        rank = 0; // if there are window elements rank will be incremented later
        bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
        bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
        bucket = &(buckets[bucketPos]);
        if (bucketPos != startBucketIdx)
            bucket->lockBucket();
        endBucketIdx = bucketPos;
        bucketElements = bucket->getElements();
    }
    while (bucketElements && (bucket->bitmapTest(bucketOffset)))
    {
        if (idx==originalDeletedIdx) // guard against full loops
            break;

        // calc hash. If its less or equal new hole position, swap, save new hole and move on
        ++rank;
        if (bucketElements)
        {
            unsigned long long idxAtRank = hasherFunc(bucketElements[rank - 1].first) % maxElements;
            if ((idxAtRank <= deletedIdx && deletedIdx - idxAtRank < maxElements / 2) || (idxAtRank > deletedIdx && idxAtRank - deletedIdx >= maxElements / 2))// TODO: this if doesnt work if mismatch is larger than half of the table
            {
                // swap
                unsigned long long swapWindowPos = deletedIdx / SparseBucket::ELEMENTS_PER_BUCKET;
                unsigned long long swapWindowOffset = deletedIdx % SparseBucket::ELEMENTS_PER_BUCKET;
                SparseBucket* swapBucket = &(buckets[swapWindowPos]);
                std::pair<const K, V>* swapWindowElements = swapBucket->getElements();
                unsigned long long swaprank = swapBucket->rankAtPos(swapWindowOffset + 1);
                std::swap((std::pair<K, V>&)swapWindowElements[swaprank - 1], (std::pair<K, V>&)bucketElements[rank - 1]); // cant swap pairs with const keys
                deletedIdx = idx;
            }
        }
        ++bucketOffset;
        idx = (idx + 1) % maxElements;
        if (bucketOffset == SparseBucket::ELEMENTS_PER_BUCKET || !idx)
        {
            rank = 0; // if there are window elements rank will be incremented later
            bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
            bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
            bucket = &(buckets[bucketPos]);
            if (bucketPos != startBucketIdx)
                bucket->lockBucket();
            endBucketIdx = bucketPos;
            bucketElements = bucket->getElements();
        }
    }

    // remove record
    unsigned long long deletedBucketPos = deletedIdx / SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long deletedBucketOffset = deletedIdx % SparseBucket::ELEMENTS_PER_BUCKET;
    SparseBucket * deletedBucket = &(buckets[deletedBucketPos]);
    std::pair<const K, V>* deletedBucketElements = deletedBucket->getElements();;
    unsigned long long deletedCount = deletedBucket->rankAtPos(SparseBucket::ELEMENTS_PER_BUCKET);

    if (deletedCount == 1)
    {
        std::free(deletedBucketElements);
        deletedBucket->setElements(nullptr); // must be locked!
        deletedBucketElements = nullptr;
    }
    else
    {
        unsigned long long deletedrank = deletedBucket->rankAtPos(deletedBucketOffset + 1);
        if (deletedrank < deletedCount)
            memmove(deletedBucketElements + deletedrank - 1, deletedBucketElements + deletedrank, (deletedCount - deletedrank) * sizeof(std::pair<const K, V>));
        deletedBucketElements = (std::pair<const K, V> *) std::realloc(deletedBucketElements, (deletedCount - 1) * sizeof(std::pair<const K, V>));
        deletedBucket->setElements(deletedBucketElements); ; // must be locked!
    }
    deletedBucket->bitmapClear(deletedBucketOffset);
    unlockBuckets(startBucketIdx, endBucketIdx);
    return true;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimpleV2<K, V, HashFunc>::insert(const K & inKey, const V & inValue)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<const K, V> * bucketElements = bucket->getElements();
    if (bucketElements == 0)
    {
        bucket->setElements((std::pair<const K, V> *) std::malloc(sizeof(std::pair<const K, V>)));
        bucketElements = bucket->getElements();
        new (&(bucketElements[0])) std::pair<const K, V>(inKey, inValue);
        bucket->bitmapSet(bucketOffset);
        unlockBuckets(startBucketIdx, endBucketIdx);
        return true;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        unsigned long long rank = bucket->rankAtPos(bucketOffset + 1);
        while (true) // linear probing
        {
            bool elementExists = bucket->bitmapTest(bucketOffset);
            if (elementExists)
            {
                if (bucketElements[rank - 1].first == inKey)
                {
                    unlockBuckets(startBucketIdx, endBucketIdx);
                    return false;
                }
                else
                {
                    stepBack = true;
                    ++rank;
                    ++bucketOffset;
                    idx = (idx + 1) % maxElements;
                    if (bucketOffset == SparseBucket::ELEMENTS_PER_BUCKET || !idx)
                    {
                        rank = 1;
                        bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
                        bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
                        bucket = &(buckets[bucketPos]);
                        if (bucketPos != startBucketIdx)
                            bucket->lockBucket();
                        endBucketIdx = bucketPos;
                        bucketElements = bucket->getElements();
                        if (bucketElements == 0)
                        {
                            bucket->setElements((std::pair<const K, V> *)std::malloc(sizeof(std::pair<const K, V>)));
                            bucketElements = bucket->getElements();
                            new (&(bucketElements[0])) std::pair<const K, V>(inKey, inValue);
                            bucket->bitmapSet(bucketOffset);
                            unlockBuckets(startBucketIdx, endBucketIdx);
                            return true;
                        }
                    }
                }
            }
            else
            {
                if (stepBack)
                    --rank;
                unsigned long long count = bucket->rankAtPos(SparseBucket::ELEMENTS_PER_BUCKET);
                bucketElements = (std::pair<const K, V> *) std::realloc(bucketElements, (count + 1) * sizeof(std::pair<const K, V>));
                bucket->setElements(bucketElements); // must be locked!
                bucketElements = bucket->getElements();
                if (rank < count)
                    memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(std::pair<const K, V>));
                new (&(bucketElements[rank])) std::pair<const K, V>(inKey, inValue);
                bucket->bitmapSet(bucketOffset);
                unlockBuckets(startBucketIdx, endBucketIdx);
                return true;
            }
        }
    }
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimpleV2<K, V, HashFunc>::set(const K & inKey, const V & inValue, bool locked)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
    unsigned long long lockRangeStart = bucketPos;
    unsigned long long lockRangeEnd = bucketPos;
    SparseBucket * bucket = &(buckets[bucketPos]);
    if (!locked)
        bucket->lockBucket();
    std::pair<const K, V> * bucketElements = bucket->getElements();
    unsigned long long rank = bucket->rankAtPos(bucketOffset + 1);

    while (bucket->bitmapTest(bucketOffset)) // linear probing
    {
        if (bucketElements && bucketElements[rank - 1].first == inKey)
        {
            bucketElements[rank - 1].second = inValue; // read value
            if (lockRangeStart == lockRangeEnd) // unlock element
            {
                bucket->unlockBucket();
            }
            else // or element range is there were collisions
            {
                unlockBuckets(lockRangeStart, lockRangeEnd);
            }
            return true;
        }
        else
        {
            ++rank;
            ++bucketOffset;
            idx = (idx + 1) % maxElements;
            if (bucketOffset == SparseBucket::ELEMENTS_PER_BUCKET || !idx)
            {
                rank = 1;
                bucketPos = idx / SparseBucket::ELEMENTS_PER_BUCKET;
                lockRangeEnd = bucketPos;
                bucketOffset = idx % SparseBucket::ELEMENTS_PER_BUCKET;
                bucket = &(buckets[bucketPos]);
                if (!locked && (bucketPos != lockRangeStart))
                    bucket->lockBucket();
                bucketElements = bucket->getElements();
            }
        }
    }
    if (lockRangeStart == lockRangeEnd) // unlock element
    {
        bucket->unlockBucket();
    }
    else // or element range is there were collisions
    {
        unlockBuckets(lockRangeStart, lockRangeEnd);
    }
    return false;
}