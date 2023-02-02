#pragma once

#include <functional>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <memory>

// TODO: implement save/load/rehash in a separate friend class\file LFSparseHashTableUtil
// TODO: remove size() / numElements from the table as its the only data causing sharing between thread, move it to separate method in Util class with O(N) complexity, multithreaded
// TODO: Implement support for monotonous keys for given key - truncate last bits for number of pairs to fit into cache line - hash, add truncated bits
template<typename K, typename V, class HashFunc = std::hash<K> >
class LFSparseHashTableSimple
{
    unsigned long long googlerank(const unsigned char *bm, unsigned long long pos) const noexcept
    {
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
        unsigned long long retval = 0;
        for (; pos > 8ULL; pos -= 8ULL)
            retval += bits_in_char[*bm++];
        return retval + bits_in_char[*bm & ((1ULL << pos) - 1ULL)];
    }

public:
    typedef K key_type;
    typedef V mapped_type;
    typedef std::pair<const K, V> value_type;
    typedef HashFunc hasher;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;

    static const unsigned long long HOLY_GRAIL_SIZE = 64; // CANT CHANGE THIS IN LF TABLE! HAS TO BE 64

    struct SparseBucket
    {
        unsigned long long  elementBitmap; //8
        std::pair<K, V>*    elements; //8

        inline void lockBucket()
        {
            unsigned long long newPointer, expectedPointer = ((unsigned long long) elements);
            while (true)
            {
                expectedPointer &= 0xFFFFFFFFFFFFFFFEULL; // expect last bit to be zero
                newPointer = expectedPointer | 1ULL;
                if (std::atomic_compare_exchange_strong((std::atomic_ullong*)&elements, &expectedPointer, newPointer))
                    break;
            }
        }
        inline void unlockBucket()
        {
            unsigned long long newPointer, oldPointer = ((unsigned long long) elements), expectedPointer;
            expectedPointer = oldPointer | 1ULL; // expect last bit to be one
            newPointer = expectedPointer & 0xFFFFFFFFFFFFFFFEULL;
            std::atomic_compare_exchange_strong((std::atomic_ullong*)&elements, &expectedPointer, newPointer);
        }
        inline void bitmapSet(unsigned long long pos)
        {
            elementBitmap |= (1ULL << pos);
        }
        inline void bitmapClear(unsigned long long pos)
        {
            elementBitmap &= ~(1ULL << pos);
        }
        inline bool bitmapTest(unsigned long long pos)
        {
            return elementBitmap & (1ULL << pos);
        }
    };

private:

    SparseBucket * buckets; //8
    unsigned long long maxElements; //8
    double maxLoadFactor; // 8
    double minLoadFactor; // 8
    HashFunc hasherFunc; // padded to 4
    char padding0[28]; // padding to cacheline
    // this is write accessed on every insert/remove and is on second cacheline
    std::atomic_ullong numElements;

public:
    LFSparseHashTableSimple(unsigned long long inMaxElements = 1024, double inMaxLoadFactor = 0.95, double inMinLoadFactor = 0.05, const HashFunc & inHasher = HashFunc());
    ~LFSparseHashTableSimple();
    LFSparseHashTableSimple(const LFSparseHashTableSimple& other);
    LFSparseHashTableSimple& operator= (const LFSparseHashTableSimple& other);

    // below operations are lock-free (as in based on "spinlock" instead of mutex)
    // IMPORTANT: additional flags for set and get allows for implementation of CAS, FETCH_ADD, FETCH_SUB etc. in client code.
    // Example: if (get(false)) { ++value; set ( true ) } is equivalent to FETCH_ADD
    // Example: if (get(false) && (value==expected)) set ( true ); is equivalent to CAS
    bool insert(const K & inKey, const V & inValue/*, bool unlock = true*/);
    bool insertOrSet(const K & inKey, const V & inValue/*, bool unlock = true*/);
    bool get(const K & inKey, V & inValue, bool unlock = true); // NOTE: if called with unlock == false, it will try to get the element and leave it locked if found
    bool set(const K & inKey, const V & inValue, bool locked = false); // NOTE: if called with locked == true, it will not try to lock element but will unlock it after set
    bool remove(const K & inKey);
    size_t size() const;
    void unlockBuckets(unsigned long long startBucketIdx, unsigned long long endBucketIdx);

    // these will come useful
    bool lockElement(const K & inKey);
    bool unlockElement(const K & inKey);

    double getLoadFactor();
    void rehash(bool grow);
    void swap(LFSparseHashTableSimple& other);
};



template<typename K, typename V, class HashFunc>
size_t LFSparseHashTableSimple<K, V, HashFunc>::size() const
{
    return numElements;
}

template<typename K, typename V, class HashFunc>
void LFSparseHashTableSimple<K, V, HashFunc>::unlockBuckets(unsigned long long startBucketIdx, unsigned long long endBucketIdx)
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

        tmp = maxElements / HOLY_GRAIL_SIZE + (maxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
        while(tmp > startBucketIdx)
        {
            buckets[--tmp].unlockBucket();
        };
    }
}

template<typename K, typename V, class HashFunc>
void LFSparseHashTableSimple<K, V, HashFunc>::swap(LFSparseHashTableSimple<K, V, HashFunc>& other)
{
    std::swap(hasherFunc, other.hasherFunc);
    std::swap(buckets, other.buckets);
    std::swap(maxElements, other.maxElements);
    other.numElements.store(numElements.exchange(other.numElements.load()));
    std::swap(maxLoadFactor, other.maxLoadFactor);
    std::swap(minLoadFactor, other.minLoadFactor);
}

template<typename K, typename V, class HashFunc>
void LFSparseHashTableSimple<K, V, HashFunc>::rehash(bool grow)
{
    LFSparseHashTableSimple<K, V, HashFunc> tmpTable((unsigned long long)ceil((numElements) / (grow ? minLoadFactor : maxLoadFactor)) + 1, maxLoadFactor, minLoadFactor);
    for (unsigned long long bucketPos = 0; bucketPos < maxElements / HOLY_GRAIL_SIZE + 1; ++bucketPos)
    {
        SparseBucket * bucket = &(buckets[bucketPos]);
        std::pair<K, V> * bucketElements = bucket->elements; // rehash should operate when no buckets are locked
        unsigned long long count = googlerank((const unsigned char*)&(bucket->elementBitmap), HOLY_GRAIL_SIZE);
        for (unsigned long long bucketElement = 0; bucketElement < count; ++bucketElement)
        {
            tmpTable.insertOrSet(bucketElements[bucketElement].first, bucketElements[bucketElement].second);
        }
    }
    swap(tmpTable);
}

template<typename K, typename V, class HashFunc>
double LFSparseHashTableSimple<K, V, HashFunc>::getLoadFactor()
{
    return ((double)numElements) / maxElements;
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimple<K, V, HashFunc>::LFSparseHashTableSimple(unsigned long long inMaxElements, double inMaxLoadFactor, double inMinLoadFactor,
    const HashFunc & inHasher) :
    buckets(0), maxElements(inMaxElements), maxLoadFactor(inMaxLoadFactor), minLoadFactor(
        inMinLoadFactor), hasherFunc(inHasher), numElements(0)
{
    if (minLoadFactor >= maxLoadFactor)
        minLoadFactor = maxLoadFactor / 2.0;
    buckets = (SparseBucket*)std::malloc((maxElements / HOLY_GRAIL_SIZE + 1) * sizeof(SparseBucket));
    memset(buckets, 0, (maxElements / HOLY_GRAIL_SIZE + 1) * sizeof(SparseBucket));
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimple<K, V, HashFunc>::~LFSparseHashTableSimple()
{
    for (unsigned long long bucketPos = 0; bucketPos < maxElements / HOLY_GRAIL_SIZE + 1; ++bucketPos)
    {
        SparseBucket * bucket = &(buckets[bucketPos]);
        unsigned long long count = googlerank((const unsigned char*)&(bucket->elementBitmap), HOLY_GRAIL_SIZE);
        std::pair<K, V> * bucketElements = bucket->elements; // destructor should work when no buckets are locked
        for (unsigned long long j = 0; j < count; ++j)
        {
            bucketElements[j].~pair();
        }
        std::free(bucket->elements);
    }
    std::free(buckets);
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimple<K, V, HashFunc>::LFSparseHashTableSimple(const LFSparseHashTableSimple& other) :
    buckets(0), maxElements(other.maxElements), maxLoadFactor(other.maxLoadFactor), minLoadFactor(other.minLoadFactor), hasherFunc(0), numElements(0)
{
    if (minLoadFactor >= maxLoadFactor)
        minLoadFactor = maxLoadFactor / 2.0;
    buckets = (SparseBucket*)std::malloc((maxElements / HOLY_GRAIL_SIZE + 1) * sizeof(SparseBucket));
    memset(buckets, 0, (maxElements / HOLY_GRAIL_SIZE + 1) * sizeof(SparseBucket));
    for (unsigned long long bucketPos = 0; bucketPos < other.maxElements / HOLY_GRAIL_SIZE + 1; ++bucketPos)
    {
        buckets[bucketPos] = other.buckets[bucketPos];
        unsigned long long count = googlerank((const unsigned char*)&(buckets[bucketPos]->elementBitmap), HOLY_GRAIL_SIZE);
        buckets[bucketPos].elements = (std::pair<K, V> *) std::malloc(count * sizeof(std::pair<K, V>)); // all buckets are created unlocked
        memcpy(buckets[bucketPos].elements, other.buckets[bucketPos].elements, count * sizeof(std::pair<K, V>)); // other table buckets should be unlocked
    }
}

template<typename K, typename V, class HashFunc>
LFSparseHashTableSimple<K, V, HashFunc>& LFSparseHashTableSimple<K, V, HashFunc>::operator= (const LFSparseHashTableSimple& other)
{
    if (&other != this)
    {
        LFSparseHashTableSimple<K, V, HashFunc> tmpTable(other);
        swap(tmpTable);
    }
    return *this;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimple<K, V, HashFunc>::insertOrSet(const K & inKey, const V & inValue)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<K, V> * bucketElements = bucket->elements;
    bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    if (bucketElements == 0)
    {
        bucketElements = (std::pair<K, V> *) std::malloc(sizeof(std::pair<K, V>));
        bucket->elements = (std::pair<K, V> *)(((unsigned long long)bucketElements) | 1ULL);
        new (&(bucketElements[0].first)) K(inKey);
        new (&(bucketElements[0].second)) V(inValue);
        bucket->bitmapSet(bucketOffset);
        ++numElements;
        unlockBuckets(startBucketIdx, endBucketIdx);
        if (getLoadFactor() > maxLoadFactor)
            rehash(true);
        return false;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        unsigned long long rank = googlerank((const unsigned char*)&(bucket->elementBitmap), bucketOffset + 1);
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
                    if (bucketOffset == HOLY_GRAIL_SIZE || !idx)
                    {
                        rank = 1;
                        bucketPos = idx / HOLY_GRAIL_SIZE;
                        bucketOffset = idx % HOLY_GRAIL_SIZE;
                        bucket = &(buckets[bucketPos]);
                        if (bucketPos != startBucketIdx)
                            bucket->lockBucket();
                        endBucketIdx = bucketPos;
                        bucketElements = bucket->elements;
                        bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
                        if (bucketElements == 0)
                        {
                            bucketElements = (std::pair<K, V> *) std::malloc(sizeof(std::pair<K, V>));
                            bucket->elements = (std::pair<K, V> *)(((unsigned long long)bucketElements) | 1ULL);
                            new (&(bucketElements[0].first)) K(inKey);
                            new (&(bucketElements[0].second)) V(inValue);
                            bucket->bitmapSet(bucketOffset);
                            ++numElements;
                            unlockBuckets(startBucketIdx, endBucketIdx);
                            if (getLoadFactor() > maxLoadFactor)
                                rehash(true);
                            return false;
                        }
                    }
                }
            }
            else
            {
                if (stepBack)
                    --rank;
                unsigned long long count = googlerank((const unsigned char*)&(bucket->elementBitmap), HOLY_GRAIL_SIZE);
                bucketElements = (std::pair<K, V> *) std::realloc(bucketElements, (count + 1) * sizeof(std::pair<K, V>));
                bucket->elements = (std::pair<K, V> *)(((unsigned long long)bucketElements) | 1ULL);
                if (rank < count)
                    memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(std::pair<K, V>));
                new (&(bucketElements[rank].first)) K(inKey);
                new (&(bucketElements[rank].second)) V(inValue);
                bucket->bitmapSet(bucketOffset);
                ++numElements;
                unlockBuckets(startBucketIdx, endBucketIdx);
                if (getLoadFactor() > maxLoadFactor)
                    rehash(true);
                return false;
            }
        }
    }
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimple<K, V, HashFunc>::get(const K & inKey, V & value, bool unlock)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    unsigned long long lockRangeStart = bucketPos;
    unsigned long long lockRangeEnd = bucketPos;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<K, V> * bucketElements = bucket->elements;
    bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    unsigned long long rank = googlerank((const unsigned char*)&(bucket->elementBitmap), bucketOffset + 1);
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
            if (bucketOffset == HOLY_GRAIL_SIZE || !idx)
            {
                rank = 1;
                bucketPos = idx / HOLY_GRAIL_SIZE;
                lockRangeEnd = bucketPos;
                bucketOffset = idx % HOLY_GRAIL_SIZE;
                bucket = &(buckets[bucketPos]);
                if (bucketPos != lockRangeStart)
                    bucket->lockBucket();
                bucketElements = bucket->elements;
                bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
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
bool LFSparseHashTableSimple<K, V, HashFunc>::remove(const K & inKey)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<K, V> * bucketElements = bucket->elements;
    bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    bool elementFound = false;

    // first, find the matching record
    unsigned long long rank;
    while (bucket->bitmapTest(bucketOffset))
    {
        if (bucketElements)
            rank = googlerank((const unsigned char*)&(bucket->elementBitmap), bucketOffset + 1);
        if (bucketElements && bucketElements[rank - 1].first == inKey)
        {
            elementFound = true;
            break;
        }
        ++bucketOffset;
        idx = (idx + 1) % maxElements;
        if (bucketOffset == HOLY_GRAIL_SIZE || !idx)
        {
            bucketPos = idx / HOLY_GRAIL_SIZE;
            bucketOffset = idx % HOLY_GRAIL_SIZE;
            bucket = &(buckets[bucketPos]);
            if (bucketPos != startBucketIdx)
                bucket->lockBucket();
            endBucketIdx = bucketPos;
            bucketElements = bucket->elements;
            bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
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
    if (bucketOffset == HOLY_GRAIL_SIZE || !idx)
    {
        rank = 0; // if there are window elements rank will be incremented later
        bucketPos = idx / HOLY_GRAIL_SIZE;
        bucketOffset = idx % HOLY_GRAIL_SIZE;
        bucket = &(buckets[bucketPos]);
        if (bucketPos != startBucketIdx)
            bucket->lockBucket();
        endBucketIdx = bucketPos;
        bucketElements = bucket->elements;
        bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    }
    while (bucketElements && (bucket->bitmapTest(bucketOffset)))
    {
        if (idx==originalDeletedIdx) // guard against full loops
            break;

        // calc hash. If its less or equal new hole position, swap, save new hole and move on
        ++rank;
        if (bucketElements)
        {
            size_t idxAtRank = hasherFunc(bucketElements[rank - 1].first) % maxElements;
            if ((idxAtRank <= deletedIdx && deletedIdx - idxAtRank < maxElements/2) || (idxAtRank > deletedIdx && idxAtRank - deletedIdx >= maxElements / 2))// TODO: this if doesnt work if the table was wrapped after deletion.
            {
                // swap
                unsigned int swapWindowPos = deletedIdx / HOLY_GRAIL_SIZE;
                unsigned int swapWindowOffset = deletedIdx % HOLY_GRAIL_SIZE;
                SparseBucket* swapBucket = &(buckets[swapWindowPos]);
                std::pair<K, V>* swapWindowElements = swapBucket->elements;
                swapWindowElements = (std::pair<K, V> *)(((unsigned long long)swapWindowElements) & 0xFFFFFFFFFFFFFFFEULL);
                unsigned long long swaprank = googlerank((const unsigned char*)&(swapBucket->elementBitmap), swapWindowOffset + 1);
                swapWindowElements[swaprank - 1] = bucketElements[rank - 1];
                deletedIdx = idx;
            }
        }
        ++bucketOffset;
        idx = (idx + 1) % maxElements;
        if (bucketOffset == HOLY_GRAIL_SIZE || !idx)
        {
            rank = 0; // if there are window elements rank will be incremented later
            bucketPos = idx / HOLY_GRAIL_SIZE;
            bucketOffset = idx % HOLY_GRAIL_SIZE;
            bucket = &(buckets[bucketPos]);
            if (bucketPos != startBucketIdx)
                bucket->lockBucket();
            endBucketIdx = bucketPos;
            bucketElements = bucket->elements;
            bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
        }
    }

    // remove record
    unsigned long long deletedBucketPos = deletedIdx / HOLY_GRAIL_SIZE;
    unsigned long long deletedBucketOffset = deletedIdx % HOLY_GRAIL_SIZE;
    SparseBucket * deletedBucket = &(buckets[deletedBucketPos]);
    std::pair<K, V> * deletedBucketElements = deletedBucket->elements;
    deletedBucketElements = (std::pair<K, V> *)(((unsigned long long)deletedBucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    unsigned long long deletedCount = googlerank((const unsigned char*)&(deletedBucket->elementBitmap), HOLY_GRAIL_SIZE);
    if (deletedCount == 1)
    {
        std::free(deletedBucketElements);
        deletedBucket->elements = (std::pair<K, V> *)1ULL;
        deletedBucketElements = 0;
    }
    else
    {
        unsigned long long deletedrank = googlerank((const unsigned char*)&(deletedBucket->elementBitmap), deletedBucketOffset + 1);
        if (deletedrank < deletedCount)
            memmove(deletedBucketElements + deletedrank - 1, deletedBucketElements + deletedrank, (deletedCount - deletedrank) * sizeof(std::pair<K, V>));
        deletedBucketElements = (std::pair<K, V> *) std::realloc(deletedBucketElements, (deletedCount - 1) * sizeof(std::pair<K, V>));
        deletedBucket->elements = deletedBucketElements;
        deletedBucket->elements = (std::pair<K, V> *)(((unsigned long long)deletedBucket->elements) | 1ULL);
    }
    deletedBucket->bitmapClear(deletedBucketOffset);
    --numElements;
    unlockBuckets(startBucketIdx, endBucketIdx);
    if (getLoadFactor() < minLoadFactor)
        rehash(false);
    return true;
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimple<K, V, HashFunc>::insert(const K & inKey, const V & inValue)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long startBucketIdx = bucketPos, endBucketIdx = bucketPos;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseBucket * bucket = &(buckets[bucketPos]);
    bucket->lockBucket();
    std::pair<K, V> * bucketElements = bucket->elements;
    bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    if (bucketElements == 0)
    {
        bucket->elements = (std::pair<K, V> *) std::malloc(sizeof(std::pair<K, V>));
        bucketElements = bucket->elements;
        bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
        new (&(bucketElements[0].first)) K(inKey);
        new (&(bucketElements[0].second)) V(inValue);
        bucket->bitmapSet(bucketOffset);
        ++numElements;
        unlockBuckets(startBucketIdx, endBucketIdx);
        if (getLoadFactor() > maxLoadFactor)
            rehash(true);
        return true;
    }
    else
    {
        bool stepBack = false; // to avoid extra rank calculation later
        unsigned long long rank = googlerank((const unsigned char*)&(bucket->elementBitmap), bucketOffset + 1);
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
                    if (bucketOffset == HOLY_GRAIL_SIZE || !idx)
                    {
                        rank = 1;
                        bucketPos = idx / HOLY_GRAIL_SIZE;
                        bucketOffset = idx % HOLY_GRAIL_SIZE;
                        bucket = &(buckets[bucketPos]);
                        if (bucketPos != startBucketIdx)
                            bucket->lockBucket();
                        endBucketIdx = bucketPos;
                        bucketElements = bucket->elements;
                        bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
                        if (bucketElements == 0)
                        {
                            bucketElements = (std::pair<K, V> *) std::malloc(sizeof(std::pair<K, V>));
                            bucket->elements = (std::pair<K, V> *)(((unsigned long long)bucketElements) | 1ULL);
                            new (&(bucketElements[0].first)) K(inKey);
                            new (&(bucketElements[0].second)) V(inValue);
                            bucket->bitmapSet(bucketOffset);
                            ++numElements;
                            unlockBuckets(startBucketIdx, endBucketIdx);
                            if (getLoadFactor() > maxLoadFactor)
                                rehash(true);
                            return true;
                        }
                    }
                }
            }
            else
            {
                if (stepBack)
                    --rank;
                unsigned long long count = googlerank((const unsigned char*)&(bucket->elementBitmap), HOLY_GRAIL_SIZE);
                bucketElements = (std::pair<K, V> *) std::realloc(bucketElements, (count + 1) * sizeof(std::pair<K, V>));
                bucket->elements = (std::pair<K, V> *)(((unsigned long long)bucketElements) | 1ULL);
                if (rank < count)
                    memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(std::pair<K, V>));
                new (&(bucketElements[rank].first)) K(inKey);
                new (&(bucketElements[rank].second)) V(inValue);
                bucket->bitmapSet(bucketOffset);
                ++numElements;
                unlockBuckets(startBucketIdx, endBucketIdx);
                if (getLoadFactor() > maxLoadFactor)
                    rehash(true);
                return true;
            }
        }
    }
}

template<typename K, typename V, class HashFunc>
bool LFSparseHashTableSimple<K, V, HashFunc>::set(const K & inKey, const V & inValue, bool locked)
{
    unsigned long long idx = hasherFunc(inKey) % maxElements; // TODO: seed
    unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    unsigned long long lockRangeStart = bucketPos;
    unsigned long long lockRangeEnd = bucketPos;
    SparseBucket * bucket = &(buckets[bucketPos]);
    if (!locked)
        bucket->lockBucket();
    std::pair<K, V> * bucketElements = bucket->elements;
    bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
    unsigned long long rank = googlerank((const unsigned char*)&(bucket->elementBitmap), bucketOffset + 1);
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
            if (bucketOffset == HOLY_GRAIL_SIZE || !idx)
            {
                rank = 1;
                bucketPos = idx / HOLY_GRAIL_SIZE;
                lockRangeEnd = bucketPos;
                bucketOffset = idx % HOLY_GRAIL_SIZE;
                bucket = &(buckets[bucketPos]);
                if (!locked && (bucketPos != lockRangeStart))
                    bucket->lockBucket();
                bucketElements = bucket->elements;
                bucketElements = (std::pair<K, V> *)(((unsigned long long)bucketElements) & 0xFFFFFFFFFFFFFFFEULL);
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
