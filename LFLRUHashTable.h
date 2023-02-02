#ifndef LFLRUHASHTABLE_H_
#define LFLRUHASHTABLE_H_

#include <functional>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <memory>
#include <math.h>

// TODO: bring back expTime. for each thread keep a possible evict list that consist of items this thread inserted and their expTime was below that of lruLists.tail.expTime at the moment of insert. check the list before popping tail.
template<typename K, typename V, class HashFunc = std::hash<K> >
class LFLRUHashTable
{
    template<typename KK, typename VV, class HH>
    friend class LFLRUHashTableUtil;

public:

    union LRULinks
    {
        struct
        {
            unsigned int left;
            unsigned int right;

        };
        struct
        {
            unsigned long long both;
        };
    };
    // K and V better be 8 bytes aligned
    struct LRUElement
    {
        template<typename KK, typename VV, class HH>
        friend class LFLRUHashTableUtil;

        std::atomic<LRULinks> links;

        K key;
        V value;
    };

    typedef K key_type;
    typedef V mapped_type;
    typedef LRUElement value_type;
    typedef HashFunc hasher;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    static const unsigned long long HASH_MAX_LOCK_DEPTH = 248;

private:

    static const unsigned long long HOLY_GRAIL_SIZE = 16;
    static const unsigned int ACCESS_LOCK_MARK = 0xFFFFFFFF;
    static const unsigned int LIST_END_MARK = 0xFFFFFFFE;
    static const unsigned int HASH_FREE_MARK = 0xFFFFFFFF;
    static const unsigned int HASH_LOCK_MARK = 0xFFFFFFFE;

    struct ThreadLRUList
    {
        std::atomic_uint head; //4
        std::atomic_uint tail; //4
        std::atomic_uint freeListHead; //4
        unsigned int reserved; //4
        unsigned long long moves; //8
        unsigned long long inserts; //8
        unsigned int locks[HASH_MAX_LOCK_DEPTH]; // four cachelines
    }__attribute((aligned(64)));

    static_assert(sizeof(std::atomic_uint) == sizeof(unsigned int), "sizeof(std::atomic_uint) != sizeof(unsigned int)");

    ThreadLRUList * lruLists; //8
    LRUElement * elementStorage; //8
    std::atomic_uint * elementLinks; //8
    unsigned long long numElements; //8
    unsigned long long hashSize; //8
    unsigned long long numThreads; //8
    std::atomic_size_t nextThreadId; //8
    std::atomic_uint globalFreeListHead; //4
    HashFunc hasherFunc; // padded to 4

    inline bool unlinkElement(unsigned int elementIdx);
    inline unsigned int unlinkTail(unsigned long long threadIdx);
    inline bool linkHead(unsigned long long threadIdx, unsigned int elementIdx);
    inline bool linkTail(unsigned long long threadIdx, unsigned int elementIdx);
    inline unsigned int lockElementHash(unsigned long long pos);
    inline unsigned int tryLockElementHash(unsigned long long pos);
    inline void unlockElementHash(unsigned long long pos, unsigned int idx);
    inline void unlockElementsHash(const unsigned long long begin, const unsigned long long end, unsigned int * indexes);
    inline void removeFromHashPos(unsigned long long threadIdx, unsigned long long storagePos);
    inline bool removeFromHashKey(unsigned long long threadIdx, const K & inKey, unsigned int& pos);

public:
    LFLRUHashTable(unsigned long long inNumElements, unsigned long long inHashSize, unsigned long long inNumThreads, const HashFunc & inHasher = HashFunc());
    ~LFLRUHashTable();
    LFLRUHashTable(const LFLRUHashTable& other);
    LFLRUHashTable& operator=(const LFLRUHashTable& other);

    unsigned long long getMyThreadId();

    // below operations are wait-free (as in based on "spinlock" instead of mutex)
    // IMPORTANT: additional flags for set and get allows for implementation of CAS, FETCH_ADD, FETCH_SUB etc. in client code.
    // Example: if (get(false)) { ++value; set ( true ) } is equivalent to FETCH_ADD
    // Example: if (get(false) && (value==expected)) set ( true ); is equivalent to CAS
    inline bool insert(unsigned long long threadIdx, const K & inKey, const V & inValue/*, bool unlock = true*/);
    inline bool insertOrSet(unsigned long long threadIdx, const K & inKey, const V & inValue/*, bool unlock = true*/); // NOTE: true if inserted
    inline bool get(unsigned long long threadIdx, const K & inKey, V & inValue, unsigned int * casLocks = 0, unsigned long long locksStart = 0,
                    unsigned long long locksEnd = 0); // casLocks must be unsigned int[HASH_MAX_LOCK_DEPTH]
    inline bool set(unsigned long long threadIdx, const K & inKey, const V & inValue, unsigned int * casLocks = 0, unsigned long long locksStart = 0,
                    unsigned long long locksEnd = 0); // casLocks must be unsigned int[HASH_MAX_LOCK_DEPTH]
    inline bool remove(unsigned long long threadIdx, const K & inKey);
    inline bool lockElement(unsigned long long threadIdx, const K & inKey, unsigned int * casLocks, unsigned long long locksStart = 0,
                            unsigned long long locksEnd = 0); // casLocks must be unsigned int[HASH_MAX_LOCK_DEPTH]
    inline bool unlockElement(unsigned long long threadIdx, const K & inKey, unsigned int * casLocks, unsigned long long locksStart = 0,
                              unsigned long long locksEnd = 0); // casLocks must be unsigned int[HASH_MAX_LOCK_DEPTH]

    void swap(LFLRUHashTable& other);

    inline size_t packHash(unsigned long long threadIdx, std::function<bool(LRUElement&)> itemPredicate);
    inline void processHash(unsigned long long threadIdx, std::function<void(LRUElement&)> itemProcessor);
};

template<typename K, typename V, class HashFunc>
unsigned long long LFLRUHashTable<K, V, HashFunc>::getMyThreadId()
{
    unsigned long long res = nextThreadId++;
    if(res >= numThreads)
        abort(); // too many worker threads
    return res;
}

// this tries to unlink element from any thread LRU list it resides in. After successful call element has left and right links set to ACCESS_LOCK_MARK and lists are consistent
template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::unlinkElement(unsigned int elementIdx)
{
    while(true)
    {
        // get target links
        LRULinks targetLinks = elementStorage[elementIdx].links.load(std::memory_order_acquire);
        if(targetLinks.left == LIST_END_MARK || targetLinks.right == LIST_END_MARK)
            // not possible to lock elements on list ends, abort
            return false;
        else if(targetLinks.left == ACCESS_LOCK_MARK || targetLinks.right == ACCESS_LOCK_MARK)
            // not possible to lock elements on list ends, abort
            continue;
        else
        {
            LRULinks targetLockLinks;
            targetLockLinks.left = ACCESS_LOCK_MARK;
            targetLockLinks.right = ACCESS_LOCK_MARK;
            // try to lock target element
            if(!elementStorage[elementIdx].links.compare_exchange_weak(targetLinks, targetLockLinks, std::memory_order_release))
            {
                // failed to lock, retry
                // TODO: sleep for systick on retry
                continue;
            }
            else
            {
                // soft lock left element
                LRULinks leftLinks = elementStorage[targetLinks.left].links.load(std::memory_order_acquire);
                LRULinks leftLockLinks = leftLinks;
                LRULinks rightLinks = elementStorage[targetLinks.right].links.load(std::memory_order_acquire);
                LRULinks rightLockLinks = rightLinks;
                if(leftLockLinks.left == LIST_END_MARK)
                {
                    // stumbled into list head, restore links and abort
                    elementStorage[elementIdx].links.store(targetLinks, std::memory_order_release);
                    return false;
                }
                else if (leftLockLinks.left == ACCESS_LOCK_MARK || leftLockLinks.right != elementIdx)
                {
                    elementStorage[elementIdx].links.store(targetLinks, std::memory_order_release);
                    // TODO: sleep for systick on retry
                    continue;
                }
                else
                    leftLockLinks.right = ACCESS_LOCK_MARK;

                // try to lock left element
                if(!elementStorage[targetLinks.left].links.compare_exchange_weak(leftLinks, leftLockLinks, std::memory_order_release))
                {
                    // failed to lock, retry
                    elementStorage[elementIdx].links.store(targetLinks, std::memory_order_release);
                    // TODO: sleep for systick on retry
                    continue;
                }
                else
                {
                    if(rightLinks.right == LIST_END_MARK)
                    {
                        // stumbled into list tail, restore links and abort
                        elementStorage[elementIdx].links.store(targetLinks, std::memory_order_release);
                        elementStorage[targetLinks.left].links.store(leftLinks, std::memory_order_release);
                        return false;
                    }
                    else if (rightLinks.right == ACCESS_LOCK_MARK || rightLinks.left != elementIdx)
                    {
                        elementStorage[elementIdx].links.store(targetLinks, std::memory_order_release);
                        elementStorage[targetLinks.left].links.store(leftLinks, std::memory_order_release);
                        continue;
                    }
                    else
                        rightLockLinks.left = ACCESS_LOCK_MARK;

                    if(!elementStorage[targetLinks.right].links.compare_exchange_weak(rightLinks, rightLockLinks, std::memory_order_release))
                    {
                        elementStorage[elementIdx].links.store(targetLinks, std::memory_order_release);
                        elementStorage[targetLinks.left].links.store(leftLinks, std::memory_order_release);
                        continue;
                    }

                    // here we got 3 elements locked
                    LRULinks newLeftLinks = leftLockLinks, newRightLinks = rightLockLinks;
                    newLeftLinks.right = targetLinks.right;
                    newRightLinks.left = targetLinks.left;
                    // link left sibling element to right
                    do
                    {
                        if(elementStorage[targetLinks.left].links.compare_exchange_weak(leftLockLinks, newLeftLinks, std::memory_order_release))
                            break;
                        else
                        {
                            leftLockLinks = elementStorage[targetLinks.left].links.load(std::memory_order_acquire);
                            newLeftLinks.left = leftLockLinks.left;
                        }
                    }
                    while(true);
                    // link right sibling element to left
                    do
                    {
                        if(elementStorage[targetLinks.right].links.compare_exchange_weak(rightLockLinks, newRightLinks, std::memory_order_release))
                            break;
                        else
                        {
                            rightLockLinks = elementStorage[targetLinks.right].links.load(std::memory_order_acquire);
                            newRightLinks.right = rightLockLinks.right;
                        }
                    }
                    while(true);
                    // target element unlinked!
                    targetLinks.left = LIST_END_MARK;
                    targetLinks.right = LIST_END_MARK;
                    elementStorage[elementIdx].links.store(targetLinks, std::memory_order_release);
                    return true;
                }
            }
        }
    }
}

template<typename K, typename V, class HashFunc>
unsigned int LFLRUHashTable<K, V, HashFunc>::unlinkTail(unsigned long long threadIdx)
{
    unsigned int oldTailIdx = lruLists[threadIdx].tail;
    while (true)
    {
        // no one operates on old tail so memory order is relaxed
        LRULinks oldTailLinks = elementStorage[oldTailIdx].links.load(std::memory_order_relaxed);
        if (oldTailLinks.left == ACCESS_LOCK_MARK || oldTailLinks.right != LIST_END_MARK)
            continue;
        if (oldTailLinks.left != LIST_END_MARK)
        {
            LRULinks oldLeftLinks = elementStorage[oldTailLinks.left].links.load(std::memory_order_acquire);
            if (oldLeftLinks.left == ACCESS_LOCK_MARK || oldLeftLinks.right != oldTailIdx)
                continue;
            LRULinks newLeftLinks = oldLeftLinks;
            newLeftLinks.right = LIST_END_MARK;
            do
            {
                if(oldLeftLinks.left != ACCESS_LOCK_MARK && oldLeftLinks.right == oldTailIdx && elementStorage[oldTailLinks.left].links.compare_exchange_weak(oldLeftLinks, newLeftLinks, std::memory_order_release))
                    break;
                else
                {
                    oldLeftLinks = elementStorage[oldTailLinks.left].links.load(std::memory_order_acquire);
                    newLeftLinks = oldLeftLinks;
                    newLeftLinks.right = LIST_END_MARK;
                }
            }
            while(true);
            lruLists[threadIdx].tail = oldTailLinks.left;
        }
        else
        {
            lruLists[threadIdx].tail = LIST_END_MARK;
            lruLists[threadIdx].head = LIST_END_MARK;
        }
        // set unlinked element links to locked state
        oldTailLinks.left = LIST_END_MARK;
        oldTailLinks.right = LIST_END_MARK;
        elementStorage[oldTailIdx].links.store(oldTailLinks, std::memory_order_relaxed);
        break;
    }
    return oldTailIdx;
}

template<typename K, typename V, class HashFunc>
void LFLRUHashTable<K, V, HashFunc>::removeFromHashPos(unsigned long long threadIdx, unsigned long long storagePos)
{
    // element hash idx
    unsigned long long idx = hasherFunc(elementStorage[storagePos].key) % hashSize; // TODO: seed
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    unsigned long long lockDepth = 0;
    unsigned long long lockDepthDeleted = HASH_MAX_LOCK_DEPTH;
    unsigned long long deletedIdx = 0;
    unsigned int * locks = lruLists[threadIdx].locks;
    locks[lockDepth] = lockElementHash(idx);

    // first, find the matching record
    while(true) // linear probing
    {
        if(elementStorage[locks[lockDepth]].key == elementStorage[storagePos].key)
        {
            deletedIdx = idx;
            lockDepthDeleted = lockDepth;
            break;
        }
        else
        {
            idx = (idx + 1) % hashSize;
            lockRangeEnd = idx;
            if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                abort();
            ++lockDepth;
            locks[lockDepth] = lockElementHash(idx);
        }
    }

    // walk forward until next hole to see if any records need to be moved back
    idx = (idx + 1) % hashSize;
    lockRangeEnd = idx;
    if (lockDepth >= HASH_MAX_LOCK_DEPTH)
        abort();
    ++lockDepth;
    locks[lockDepth] = lockElementHash(idx);
    while(locks[lockDepth] != HASH_FREE_MARK) // linear probing
    {
        // calc hash. If its less or equal new hole position, swap, save new hole and move on
        unsigned long long nextHash = hasherFunc(elementStorage[locks[lockDepth]].key);
        if(nextHash % hashSize <= deletedIdx)
        {
            // swap
            locks[lockDepthDeleted] = locks[lockDepth];
            lockDepthDeleted = lockDepth;
            deletedIdx = idx;
        }
        idx = (idx + 1) % hashSize;
        lockRangeEnd = idx;
        if (lockDepth >= HASH_MAX_LOCK_DEPTH)
            abort();
        ++lockDepth;
        locks[lockDepth] = lockElementHash(idx);
    }
    if(lockDepthDeleted != HASH_MAX_LOCK_DEPTH)
        locks[lockDepthDeleted] = HASH_FREE_MARK;
    unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::removeFromHashKey(unsigned long long threadIdx, const K& inKey, unsigned int& pos)
{
    // element hash idx
    unsigned long long idx = hasherFunc(inKey) % hashSize; // TODO: seed
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    unsigned long long lockDepth = 0;
    unsigned long long lockDepthDeleted = HASH_MAX_LOCK_DEPTH;
    unsigned long long deletedIdx = 0;
    unsigned int * locks = lruLists[threadIdx].locks;
    locks[lockDepth] = lockElementHash(idx);

    // first, find the matching record
    while(true) // linear probing
    {
        if(elementStorage[locks[lockDepth]].key == inKey)
        {
            deletedIdx = idx;
            lockDepthDeleted = lockDepth;
            pos = locks[lockDepthDeleted];
            break;
        }
        else
        {
            idx = (idx + 1) % hashSize;
            lockRangeEnd = idx;
            if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                abort();
            ++lockDepth;
            locks[lockDepth] = lockElementHash(idx);
        }
    }

    // if element isnt found - unlock and return
    if (lockDepthDeleted == HASH_MAX_LOCK_DEPTH)
    {
        unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
        return false;
    }

    // walk forward until next hole to see if any records need to be moved back
    idx = (idx + 1) % hashSize;
    lockRangeEnd = idx;
    if (lockDepth >= HASH_MAX_LOCK_DEPTH)
        abort();
    ++lockDepth;
    locks[lockDepth] = lockElementHash(idx);
    while(locks[lockDepth] != HASH_FREE_MARK) // linear probing
    {
        // calc hash. If its less or equal new hole position, swap, save new hole and move on
        if(hasherFunc(elementStorage[locks[lockDepth]].key) % hashSize <= deletedIdx)
        {
            // swap
            locks[lockDepthDeleted] = locks[lockDepth];
            lockDepthDeleted = lockDepth;
            deletedIdx = idx;
        }
        else
        {
            idx = (idx + 1) % hashSize;
            lockRangeEnd = idx;
            if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                abort();
            ++lockDepth;
            locks[lockDepth] = lockElementHash(idx);
        }
    }
    if(lockDepthDeleted != HASH_MAX_LOCK_DEPTH)
        locks[lockDepthDeleted] = HASH_FREE_MARK;
    unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
    return true;
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::linkHead(unsigned long long threadIdx, unsigned int elementIdx)
{
    unsigned int oldHead = lruLists[threadIdx].head;
    LRULinks newHeadLinks;
    newHeadLinks.left = LIST_END_MARK;
    newHeadLinks.right = oldHead;
    elementStorage[elementIdx].links.store(newHeadLinks, std::memory_order_relaxed);
    // no one operates on old head so memory order is relaxed
    if (oldHead != LIST_END_MARK)
    {
        LRULinks oldHeadLinks = elementStorage[oldHead].links.load(std::memory_order_relaxed);
        oldHeadLinks.left = elementIdx;
        elementStorage[oldHead].links.store(oldHeadLinks, std::memory_order_release);
    }
    else
    {
        lruLists[threadIdx].tail = elementIdx;
    }
    lruLists[threadIdx].head = elementIdx;
    return true;
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::linkTail(unsigned long long threadIdx, unsigned int elementIdx)
{
    unsigned int oldTail = lruLists[threadIdx].tail;
    LRULinks newTailLinks;
    newTailLinks.left = oldTail;
    newTailLinks.right = LIST_END_MARK;
    elementStorage[elementIdx].links.store(newTailLinks, std::memory_order_relaxed);
    // no one operates on old tail so memory order is relaxed
    if (oldTail != LIST_END_MARK)
    {
        LRULinks oldTailLinks = elementStorage[oldTail].links.load(std::memory_order_relaxed);
        oldTailLinks.right = elementIdx;
        elementStorage[oldTail].links.store(oldTailLinks, std::memory_order_release);
    }
    else
    {
        lruLists[threadIdx].head = elementIdx;
    }
    lruLists[threadIdx].tail = elementIdx;
    return true;
}

template<typename K, typename V, class HashFunc>
unsigned int LFLRUHashTable<K, V, HashFunc>::lockElementHash(unsigned long long pos)
{
    unsigned int idx = elementLinks[pos].load(std::memory_order_acquire);
    unsigned int lockMark = HASH_LOCK_MARK;
    while(true)
    {
        if(idx == HASH_LOCK_MARK)
            idx = elementLinks[pos].load(std::memory_order_acquire);
        else if(elementLinks[pos].compare_exchange_weak(idx, lockMark, std::memory_order_release))
            break;
    }
    return idx;
}
template<typename K, typename V, class HashFunc>
unsigned int LFLRUHashTable<K, V, HashFunc>::tryLockElementHash(unsigned long long pos)
{
    unsigned int idx = elementLinks[pos].load(std::memory_order_acquire);
    unsigned int lockMark = HASH_LOCK_MARK;
    while(true)
    {
        if(idx == HASH_LOCK_MARK || elementLinks[pos].compare_exchange_weak(idx, lockMark, std::memory_order_release))
            return idx;
    }
}
template<typename K, typename V, class HashFunc>
void LFLRUHashTable<K, V, HashFunc>::unlockElementHash(unsigned long long pos, unsigned int idx)
{
    elementLinks[pos].store(idx, std::memory_order_seq_cst);
}

template<typename K, typename V, class HashFunc>
void LFLRUHashTable<K, V, HashFunc>::unlockElementsHash(unsigned long long lockRangeStartIdx, unsigned long long lockRangeEndIdx, unsigned int * indexes)
{
    unsigned long long counter = 0;
    while(lockRangeEndIdx != lockRangeStartIdx)
    {
        unlockElementHash(lockRangeStartIdx, indexes[counter++]);
        if(++lockRangeStartIdx == hashSize)
            lockRangeStartIdx = 0;
    }
    unlockElementHash(lockRangeStartIdx, indexes[counter++]);
}

template<typename K, typename V, class HashFunc>
void LFLRUHashTable<K, V, HashFunc>::swap(LFLRUHashTable<K, V, HashFunc>& other)
{
    std::swap(nextThreadId, other.nextThreadId);
    std::swap(hasherFunc, other.hasherFunc);
    std::swap(elementLinks, other.elementLinks);
    std::swap(hashSize, other.hashSize);
    std::swap(numElements, other.numElements);
    std::swap(numThreads, other.numThreads);
    std::swap(lruLists, other.lruLists);
    std::swap(elementStorage, other.elementStorage);
    std::swap(globalFreeListHead, other.globalFreeListHead);
}

template<typename K, typename V, class HashFunc>
LFLRUHashTable<K, V, HashFunc>::LFLRUHashTable(unsigned long long inNumElements, unsigned long long inHashSize, unsigned long long inNumThreads,
                                               const HashFunc & inHasher) :
                lruLists(0), elementStorage(0), elementLinks(0), numElements(inNumElements), hashSize(inHashSize), numThreads(inNumThreads), globalFreeListHead(LIST_END_MARK), hasherFunc(
                                inHasher)
{
    if(numElements >= hashSize)
        abort();
    nextThreadId = 0;
    elementStorage = (LRUElement*) std::malloc(numElements * sizeof(LRUElement));
    memset(elementStorage, 0, numElements * sizeof(LRUElement));
    lruLists = (ThreadLRUList*) std::malloc(numThreads * sizeof(ThreadLRUList));
    for(unsigned long long elementIdx = 0; elementIdx < numElements; ++elementIdx)
    {
        LRULinks links;
        links.left = LIST_END_MARK;
        links.right = elementIdx + 1;
        elementStorage[elementIdx].links = links;
    }
    unsigned long long elementsPerThread = numElements / numThreads;
    for(unsigned long long threadIdx = 0; threadIdx < numThreads; ++threadIdx)
    {
        lruLists[threadIdx].head = LIST_END_MARK;
        lruLists[threadIdx].tail = LIST_END_MARK;
        lruLists[threadIdx].freeListHead = threadIdx * elementsPerThread;
        LRULinks links;
        links = elementStorage[(threadIdx + 1) * elementsPerThread - 1].links.load();
        links.right = LIST_END_MARK;
        if (links.left == (threadIdx + 1) * elementsPerThread - 1) abort();
        elementStorage[(threadIdx + 1) * elementsPerThread - 1].links.store(links, std::memory_order_seq_cst);
    }
    elementLinks = (std::atomic_uint*) std::malloc(hashSize * sizeof(std::atomic_uint));
    memset(elementLinks, 255 /*1/4 HASH_FREE_MARK*/, hashSize * sizeof(std::atomic_uint));
}

template<typename K, typename V, class HashFunc>
LFLRUHashTable<K, V, HashFunc>::~LFLRUHashTable()
{
    for(unsigned long long pos = 0; pos < hashSize; ++pos)
    {
        elementStorage[elementLinks[pos]].key.~K();
        elementStorage[elementLinks[pos]].value.~V();
    }
    std::free(lruLists);
    std::free(elementLinks);
    std::free(elementStorage);
}

template<typename K, typename V, class HashFunc>
LFLRUHashTable<K, V, HashFunc>::LFLRUHashTable(const LFLRUHashTable& other) :
                lruLists(0), elementStorage(0), elementLinks(0), numElements(other.numElements), hashSize(other.hashSize), numThreads(other.numThreads), hasherFunc(
                                other.hasherFunc)
{
    nextThreadId = other.nextThreadId;
    elementStorage = (LRUElement*) std::malloc(numElements * sizeof(LRUElement));
    elementLinks = (std::atomic_uint*) std::malloc(hashSize * sizeof(LRUElement));
    lruLists = (ThreadLRUList*) std::malloc(numThreads * sizeof(ThreadLRUList));
    memset(elementLinks, 255 /*1/4 HASH_FREE_MARK*/, hashSize * sizeof(LRUElement));
    for(unsigned long long elementIdx = 0; elementIdx < numElements; ++elementIdx)
    {
        elementStorage[elementIdx] = other.elementStorage[elementIdx];
    }
    for(unsigned long long lruIdx = 0; lruIdx < numThreads; ++lruIdx)
    {
        lruLists[lruIdx] = other.lruLists[lruIdx];
    }
    for(unsigned long long pos = 0; pos < hashSize; ++pos)
    {
        elementLinks[pos] = other.elementLinks[pos];
    }
}

template<typename K, typename V, class HashFunc>
LFLRUHashTable<K, V, HashFunc>& LFLRUHashTable<K, V, HashFunc>::operator=(const LFLRUHashTable& other)
{
    if(&other != this)
    {
        LFLRUHashTable<K, V, HashFunc> tmpTable(other);
        swap(tmpTable);
    }
    return *this;
}

// returns true if inserted, false if set
template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::insertOrSet(unsigned long long threadIdx, const K & inKey, const V & inValue)
{
    unsigned int elementToInsert = lruLists[threadIdx].freeListHead;
    LRULinks freeListHeadLinks = elementStorage[elementToInsert].links.load();
    unsigned long long nextFreeHead = freeListHeadLinks.right;
    lruLists[threadIdx].freeListHead = nextFreeHead;

    unsigned long long idx = hasherFunc(inKey) % hashSize; // TODO: seed
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    unsigned long long lockDepth = 0;
    unsigned int * locks = lruLists[threadIdx].locks;
    locks[lockDepth] = lockElementHash(idx);
    while(locks[lockDepth] != HASH_FREE_MARK) // linear probing
    {
        if(elementStorage[locks[lockDepth]].key == inKey)
        {
            elementStorage[locks[lockDepth]].value = inValue;
            unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
            if(unlinkElement(locks[lockDepth]))
            {
                ++(lruLists[threadIdx].moves);
                linkHead(threadIdx, locks[lockDepth]);
            }
            freeListHeadLinks.left = LIST_END_MARK;
            freeListHeadLinks.right = lruLists[threadIdx].freeListHead;
            elementStorage[elementToInsert].links.store(freeListHeadLinks);
            lruLists[threadIdx].freeListHead = elementToInsert;
            return false;
        }
        else
        {
            idx = (idx + 1) % hashSize;
            lockRangeEnd = idx;
            if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                abort();
            ++lockDepth;
            locks[lockDepth] = lockElementHash(idx);
        }
    }
    locks[lockDepth] = elementToInsert;
    elementStorage[elementToInsert].key = inKey;
    elementStorage[elementToInsert].value = inValue;
    unlockElementsHash(lockRangeStart, lockRangeEnd, locks);

    linkHead(threadIdx, elementToInsert);

    if (lruLists[threadIdx].freeListHead == LIST_END_MARK)
    {
        elementToInsert = unlinkTail(threadIdx);
        removeFromHashPos(threadIdx, elementToInsert);
        freeListHeadLinks = elementStorage[elementToInsert].links.load();
        freeListHeadLinks.left = LIST_END_MARK;
        freeListHeadLinks.right = lruLists[threadIdx].freeListHead;
        elementStorage[elementToInsert].links.store(freeListHeadLinks);
        lruLists[threadIdx].freeListHead = elementToInsert;
    }

    return true;

}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::get(unsigned long long threadIdx, const K & inKey, V & value, unsigned int * casLocks, unsigned long long locksStart,
                                         unsigned long long locksEnd)
{
    unsigned long long idx = hasherFunc(inKey) % hashSize; // TODO: seed
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    unsigned long long lockDepth = 0;
    unsigned int * locks = casLocks ? casLocks : lruLists[threadIdx].locks;
    locks[lockDepth] = lockElementHash(idx);

    while(locks[lockDepth] != HASH_FREE_MARK) // linear probing
    {
        if(elementStorage[locks[lockDepth]].key == inKey)
        {
            value = elementStorage[locks[lockDepth]].value; // read value
            if(!casLocks)
            {
                unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
            }
            else
            {
                locksStart = lockRangeStart;
                locksEnd = lockRangeEnd;
            }
            if(unlinkElement(locks[lockDepth]))
            {
                ++(lruLists[threadIdx].moves);
                linkHead(threadIdx, locks[lockDepth]);
            }
            return true;
        }
        else
        {
            idx = (idx + 1) % hashSize;
            lockRangeEnd = idx;
            if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                abort();
            ++lockDepth;
            locks[lockDepth] = lockElementHash(idx);
        }
    }
    if(!casLocks)
    {
        unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
    }
    else
    {
        locksStart = lockRangeStart;
        locksEnd = lockRangeEnd;
    }
    return false;
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::remove(unsigned long long threadIdx, const K & inKey)
{
    unsigned int pos;
    bool removed = removeFromHashKey(threadIdx, inKey, pos);
    if (removed)
    {
        if (unlinkElement(pos))
        {
            LRULinks freeListHeadLinks = elementStorage[pos].links.load();
            freeListHeadLinks.left = LIST_END_MARK;
            freeListHeadLinks.right = lruLists[threadIdx].freeListHead;
            elementStorage[pos].links.store(freeListHeadLinks);
            lruLists[threadIdx].freeListHead = pos;
        }
        else
        {
            // TODO: here item needs to be placed into global free list that needs to be checked when thread is out of free elements before popping threads tail
            // TODO: for now elements would eventually be reused, so leave it as is
            // abort();
        }
        return true;
    }
    return false;
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::insert(unsigned long long threadIdx, const K & inKey, const V & inValue)
{
    unsigned int elementToInsert = lruLists[threadIdx].freeListHead;
    LRULinks freeListHeadLinks = elementStorage[elementToInsert].links.load();
    unsigned long long nextFreeHead = freeListHeadLinks.right;
    lruLists[threadIdx].freeListHead = nextFreeHead;

    unsigned long long idx = hasherFunc(inKey) % hashSize; // TODO: seed
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    unsigned long long lockDepth = 0;
    unsigned int * locks = lruLists[threadIdx].locks;
    locks[lockDepth] = lockElementHash(idx);
    while(locks[lockDepth] != HASH_FREE_MARK) // linear probing
    {
        if(elementStorage[locks[lockDepth]].key == inKey)
        {
            unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
            freeListHeadLinks.left = LIST_END_MARK;
            freeListHeadLinks.right = lruLists[threadIdx].freeListHead;
            elementStorage[elementToInsert].links.store(freeListHeadLinks);
            lruLists[threadIdx].freeListHead = elementToInsert;
            return false;
        }
        else
        {
            idx = (idx + 1) % hashSize;
            lockRangeEnd = idx;
            if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                abort();
            ++lockDepth;
            locks[lockDepth] = lockElementHash(idx);
        }
    }
    locks[lockDepth] = elementToInsert;
    elementStorage[elementToInsert].key = inKey;
    elementStorage[elementToInsert].value = inValue;
    unlockElementsHash(lockRangeStart, lockRangeEnd, locks);

    linkHead(threadIdx, elementToInsert);

    if (lruLists[threadIdx].freeListHead == LIST_END_MARK)
    {
        elementToInsert = unlinkTail(threadIdx);
        removeFromHashPos(threadIdx, elementToInsert);
        freeListHeadLinks = elementStorage[elementToInsert].links.load();
        freeListHeadLinks.left = LIST_END_MARK;
        freeListHeadLinks.right = lruLists[threadIdx].freeListHead;
        elementStorage[elementToInsert].links.store(freeListHeadLinks);
        lruLists[threadIdx].freeListHead = elementToInsert;
    }

    return true;
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::set(unsigned long long threadIdx, const K & inKey, const V & inValue, unsigned int * casLocks,
                                         unsigned long long locksStart, unsigned long long locksEnd)
{
    if(casLocks)
    {
        unsigned int * locks = casLocks;
        unsigned long long lockDepth = locksEnd > locksStart ? locksEnd - locksStart + 1 : hashSize - locksStart + locksEnd;
        elementStorage[locks[lockDepth]].value = inValue;
        unlockElementsHash(locksStart, locksEnd, locks);
        if(unlinkElement(locks[lockDepth]))
        {
            ++(lruLists[threadIdx].moves);
            linkHead(threadIdx, locks[lockDepth]);
        }
        return true;
    }
    else
    {
        unsigned long long idx = hasherFunc(inKey) % hashSize; // TODO: seed
        unsigned long long lockRangeStart = idx;
        unsigned long long lockRangeEnd = idx;
        unsigned long long lockDepth = 0;
        unsigned int * locks = lruLists[threadIdx].locks;
        locks[lockDepth] = lockElementHash(idx);

        while(locks[lockDepth] != HASH_FREE_MARK) // linear probing
        {
            if(elementStorage[locks[lockDepth]].key == inKey)
            {
                elementStorage[locks[lockDepth]].value = inValue;
                unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
                if(unlinkElement(locks[lockDepth]))
                {
                    ++(lruLists[threadIdx].moves);
                    linkHead(threadIdx, locks[lockDepth]);
                }
                return true;
            }
            else
            {
                idx = (idx + 1) % hashSize;
                lockRangeEnd = idx;
                if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                    abort();
                ++lockDepth;
                locks[lockDepth] = lockElementHash(idx);
            }
        }
        unlockElementsHash(lockRangeStart, lockRangeEnd, locks);
        return false;
    }
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::lockElement(unsigned long long threadIdx, const K & inKey, unsigned int * casLocks, unsigned long long locksStart,
                                                 unsigned long long locksEnd)
{
    unsigned long long idx = hasherFunc(inKey) % hashSize; // TODO: seed
    unsigned long long lockRangeStart = idx;
    unsigned long long lockRangeEnd = idx;
    unsigned long long lockDepth = 0;
    unsigned int * locks = casLocks ? casLocks : lruLists[threadIdx].locks;
    locks[lockDepth] = lockElementHash(idx);

    while(locks[lockDepth] != HASH_FREE_MARK) // linear probing
    {
        if(elementStorage[locks[lockDepth]].key == inKey)
        {
            locksStart = lockRangeStart;
            locksEnd = lockRangeEnd;
            return true;
        }
        else
        {
            idx = (idx + 1) % hashSize;
            lockRangeEnd = idx;
            if (lockDepth >= HASH_MAX_LOCK_DEPTH)
                abort();
            ++lockDepth;
            locks[lockDepth] = lockElementHash(idx);
        }
    }
    locksStart = lockRangeStart;
    locksEnd = lockRangeEnd;
    return false;
}

template<typename K, typename V, class HashFunc>
bool LFLRUHashTable<K, V, HashFunc>::unlockElement(unsigned long long threadIdx, const K & inKey, unsigned int * casLocks, unsigned long long locksStart,
                                                   unsigned long long locksEnd)
{
    unlockElementsHash(locksStart, locksEnd, casLocks);
    return true;
}

template<typename K, typename V, class HashFunc>
void LFLRUHashTable<K, V, HashFunc>::processHash(unsigned long long threadIdx, std::function<void(LRUElement&)> itemProcessor)
{
    for (unsigned long long pos = 0; pos < hashSize; ++pos)
    {
        unsigned int idx = elementLinks[pos].load(std::memory_order_acquire);
        unsigned int lockMark = HASH_LOCK_MARK;
        while(true)
        {
            if(idx == HASH_LOCK_MARK)
                idx = elementLinks[pos].load(std::memory_order_acquire);
            else if(elementLinks[pos].compare_exchange_weak(idx, lockMark, std::memory_order_release))
                break;
        }
        itemProcessor(elementStorage[idx]);
        elementLinks[pos].store(idx, std::memory_order_acq_rel);
    }
}
#endif /* LFLRUHASHTABLE_H_ */
