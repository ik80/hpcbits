/* Copyright 2018-2019 Kalujny Ilya

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*/

#ifndef TLSOBJECTPOOL_H_
#define TLSOBJECTPOOL_H_

#include <stdlib.h>
#include <stdio.h>
#include <stdexcept>
#include <cstring>
#include <map>
#include <thread>

// Bullshittenator9000 single threaded object pool. Aquire continuous memory at the start. Have a tree of bitmaps, every chunk with size of cacheline.
// On the lowest level 1 means element is free, on upper level 1 means chunk has free elements, etc , etc. Overhead is Log512(N) bits per N elements

/* Classes stored in the pool can overload new and delete (new[] and delete[] should still work like std::malloc/std::free!) in a similar way:

 struct A
 {
 virtual ~A();
 virtual void foo();
 ...
 static void* operator new (std::size_t count)
 {
 A* pA = TLSObjectPool<A>::Aquire();
 return (void*)pA;
 }
 static void operator delete ( void* ptr )
 {
 if (ptr)
 TLSObjectPool<A>::Release((A*)ptr);
 }
 }

 struct B: public A
 {
 virtual ~B();
 virtual void foo();
 ...
 static void* operator new (std::size_t count)
 {
 B* pB = TLSObjectPool<B>::Aquire();
 return (void*)pB;
 }
 static void operator delete ( void* ptr )
 {
 if (ptr)
 TLSObjectPool<B>::Release((B*)ptr);
 }
 }

 // this is fine:
 A* pA = new B();
 pA->foo();
 delete pA;

 */



template<typename T>
class TLSObjectPool
{
public:

    static const size_t BMP_TREE_HEIGHT = 9;
    static const size_t POOL_CHUNK_SIZE = 512;
    static const size_t QWORD_BITS = 64;
    static const size_t POOL_CHUNK_SIZE_QWORDS = 8;

    static inline void bitmapSet(size_t & bitmap, size_t pos)
    {
        bitmap |= (1ULL << (pos & 63ULL));
    }
    static inline void bitmapClear(size_t & bitmap, size_t pos)
    {
        bitmap &= ~(1ULL << (pos & 63ULL));
    }
    static inline bool bitmapTest(size_t bitmap, size_t pos)
    {
        return bitmap & (1ULL << (pos & 63ULL));
    }

    static void registerThread()
    {
    	if (!numTreadsToRegister)
    		abort();
    	treadIds[std::this_thread::get_id()] = --numTreadsToRegister;
    }

    // Pool size MUST be a multiple of 512
    static void setUp(size_t in_poolSize, size_t in_numThreads)
    {
        if ((in_poolSize/in_numThreads) % POOL_CHUNK_SIZE)
            abort();

		numTreadsToRegister = in_numThreads;
		numTreads = in_numThreads;

        arrCtrlBlocks = new TLSPoolControlBlock<T>[numTreads];
    	for (size_t j = 0; j < numTreads; ++ j)
    	{
			if (arrCtrlBlocks[j].storage != nullptr)
				abort();
			arrCtrlBlocks[j].poolSize = in_poolSize/numTreads;
			arrCtrlBlocks[j].freeSize = arrCtrlBlocks[j].poolSize;
			arrCtrlBlocks[j].storage = static_cast<T*>(std::malloc(sizeof(T)*arrCtrlBlocks[j].poolSize));
			memset(arrCtrlBlocks[j].storage, 0x0, arrCtrlBlocks[j].poolSize * sizeof(T));
			size_t counter = arrCtrlBlocks[j].poolSize;
			size_t prevCounter = counter;
			int levels = 0;

			do
			{
				prevCounter = counter;
				counter /= POOL_CHUNK_SIZE;
				arrCtrlBlocks[j].treeMap[levels] = new uint64_t[(counter ? counter : 1) * POOL_CHUNK_SIZE_QWORDS ]; // bit per element
				if (prevCounter >= POOL_CHUNK_SIZE)
					memset(arrCtrlBlocks[j].treeMap[levels], 0xFF, counter * POOL_CHUNK_SIZE_QWORDS * sizeof(uint64_t));
				else
				{
					arrCtrlBlocks[j].topLevelElements = prevCounter;
					memset(arrCtrlBlocks[j].treeMap[levels], 0, POOL_CHUNK_SIZE_QWORDS * sizeof(uint64_t));
					//topmost level is partially filled, MUST set zeros at relevant bits
					size_t pos = 0;
					while (prevCounter >= QWORD_BITS)
					{
						arrCtrlBlocks[j].treeMap[levels][pos++] = (uint64_t) (-1);
						prevCounter -= QWORD_BITS;
					}
					arrCtrlBlocks[j].treeMap[levels][pos++] = (1ULL << prevCounter) - 1ULL;
				}
				++levels;
			}
			while (counter);

			arrCtrlBlocks[j].treeMapLevels = levels;

			for (size_t i = 0; i < arrCtrlBlocks[j].treeMapLevels / 2; ++i)
			{
				uint64_t * tmp = arrCtrlBlocks[j].treeMap[i];
				arrCtrlBlocks[j].treeMap[i] = arrCtrlBlocks[j].treeMap[arrCtrlBlocks[j].treeMapLevels - i - 1];
				arrCtrlBlocks[j].treeMap[arrCtrlBlocks[j].treeMapLevels - i - 1] = tmp;
			}
			arrCtrlBlocks[j].lastFreeIdx = arrCtrlBlocks[j].poolSize - 1;
    	}
    	// let the game begin
    }

    static void tearDown()
    {
    	for (size_t j = 0; j < numTreads; ++ j)
    	{
            if (arrCtrlBlocks[j].storage == nullptr)
                abort();
            std::free(arrCtrlBlocks[j].storage);
            arrCtrlBlocks[j].storage = nullptr;
            for (size_t i = 0; i < arrCtrlBlocks[j].treeMapLevels; ++i)
                delete[] arrCtrlBlocks[j].treeMap[i]; //TODO: leak checks!
    	}
    }

    // Ugh
    static T * acquire()
    {
        static thread_local TLSPoolControlBlock<T> *  const pCtrlBlk = &(arrCtrlBlocks[treadIds[std::this_thread::get_id()]]);

        T * pRes = 0;
        if (pCtrlBlk->freeSize)
        {
            //walk forward/up from last free idx
            size_t idx = (pCtrlBlk->lastFreeIdx + 1) % pCtrlBlk->poolSize;
            size_t level = pCtrlBlk->treeMapLevels - 1;
            bool wentUp = false;
            unsigned long bitPos = 0;
            do
            {
                //check the remainder of nearest ull
                const size_t mask = ((pCtrlBlk->treeMap[level][idx / QWORD_BITS]) & (((size_t) (-1)) << idx % QWORD_BITS));
                if (mask)
                {
                    bitPos = __builtin_ffsll(mask);
                    idx = (idx - idx % QWORD_BITS + bitPos - 1);
                    break;
                }
                else // check remaining ulls in the chunk
                {
                    size_t stepsToGoThisLevel = 7 - (idx % POOL_CHUNK_SIZE) / QWORD_BITS;
                    bitPos = 0;
                    while (!bitPos && stepsToGoThisLevel)
                    {
                   		if (pCtrlBlk->treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + (8 - stepsToGoThisLevel))] == 0)
                   		{
                            --stepsToGoThisLevel;
                   		}
                   		else
                   		{
                   			bitPos = __builtin_ffsll(pCtrlBlk->treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + (8 - stepsToGoThisLevel))]);
                   		}
                    }
                    if (bitPos)
                    {
                        idx = (idx - idx % POOL_CHUNK_SIZE + (8 - stepsToGoThisLevel) * QWORD_BITS + bitPos - 1);
                        break;
                    }
                }
                // no luck move up
                idx = idx / POOL_CHUNK_SIZE;
                --level;
                wentUp = true;
            }
            while (level > 0);


            if (!level)
            {
                // level zero requires special treatment
                bitPos = 0;
                // need to zero out bits pre-index on first iteration
                const size_t mask = ((pCtrlBlk->treeMap[0][idx / QWORD_BITS]) & (((size_t) (-1)) << idx % QWORD_BITS));
                if (mask)
                {
                    bitPos = __builtin_ffsll(mask);
                    idx = (idx - idx % QWORD_BITS + bitPos - 1);
                }
                else // and then wrap the whole top level starting from next position. UGLY CODE!
                {
                    size_t stepsThisLevel = pCtrlBlk->topLevelElements / QWORD_BITS + 1;
                    idx = ((idx / QWORD_BITS + 1) * QWORD_BITS) % pCtrlBlk->topLevelElements;
                    while (stepsThisLevel && pCtrlBlk->treeMap[0][(idx/QWORD_BITS + pCtrlBlk->topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (pCtrlBlk->topLevelElements / QWORD_BITS + 1)] == 0)
                        --stepsThisLevel;
                    bitPos = __builtin_ffsll(pCtrlBlk->treeMap[0][(idx/QWORD_BITS + pCtrlBlk->topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (pCtrlBlk->topLevelElements / QWORD_BITS + 1)]);
                    idx = ((idx/QWORD_BITS + pCtrlBlk->topLevelElements / QWORD_BITS + 1 - stepsThisLevel) % (pCtrlBlk->topLevelElements / QWORD_BITS + 1) * QWORD_BITS + bitPos - 1);
                }
                ++level;
                if (level < pCtrlBlk->treeMapLevels)
                    idx *= POOL_CHUNK_SIZE;
            }

            //and now forward/down to the next free idx
            if (wentUp || level < pCtrlBlk->treeMapLevels - 1)
            {
                do
                {
                    bitPos = 0;
                    size_t stepsThisLevel = 0;
                    while (pCtrlBlk->treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + stepsThisLevel)] == 0)
                        ++stepsThisLevel;
                    bitPos = __builtin_ffsll(pCtrlBlk->treeMap[level][((idx - idx % POOL_CHUNK_SIZE) / QWORD_BITS + stepsThisLevel)]);
                    idx = ((idx - idx % POOL_CHUNK_SIZE) + stepsThisLevel * QWORD_BITS + bitPos - 1);
                    ++level;
                    if (level < pCtrlBlk->treeMapLevels)
                        idx *= POOL_CHUNK_SIZE;
                }
                while (level < pCtrlBlk->treeMapLevels);
            }

            //save previous free position
            pCtrlBlk->lastFreeIdx = idx;
            pRes = &(pCtrlBlk->storage[idx]);
            --pCtrlBlk->freeSize;
            //set zero bits upwards as needed
            level = pCtrlBlk->treeMapLevels - 1;
            while (level >= 0 && level < pCtrlBlk->treeMapLevels)
            {
                bitmapClear(pCtrlBlk->treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS);
                bool done = false;
                //check neighbors
                size_t chunkStart = idx - (idx % POOL_CHUNK_SIZE);
                for (size_t nearIdx = 0; nearIdx < 8 && !done; ++nearIdx)
                    if (pCtrlBlk->treeMap[level][chunkStart / QWORD_BITS + nearIdx])
                        done = true;
                if (!done)
                {
                    idx /= POOL_CHUNK_SIZE;
                    --level;
                }
                else
                    break;
            }
        }
        return pRes;
    }

    static void release(T * objPtr)
    {
        static thread_local TLSPoolControlBlock<T> * const pCtrlBlk = &(arrCtrlBlocks[treadIds[std::this_thread::get_id()]]);
        // objPtr - storage == idx. set one bits upwards as needed
        size_t idx = objPtr - pCtrlBlk->storage;
        size_t level = pCtrlBlk->treeMapLevels - 1;
        while (level >= 0 && level < pCtrlBlk->treeMapLevels)
        {
            if (!bitmapTest(pCtrlBlk->treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS))
            {
                bitmapSet(pCtrlBlk->treeMap[level][idx / QWORD_BITS], idx % QWORD_BITS);
                --level;
                idx /= POOL_CHUNK_SIZE;
            }
            else
                break;
        }
        ++pCtrlBlk->freeSize;
    }

    template<typename U>
    struct TLSPoolControlBlock
	{
        uint64_t * treeMap[BMP_TREE_HEIGHT]; // 72
        U * storage; // 80
        size_t treeMapLevels; // 88
        size_t poolSize; // 96
        size_t freeSize; // 104
        size_t lastFreeIdx; // 112
        size_t topLevelElements; // 120
        size_t padding; // 128, two cache lines
	};

private:

    static TLSPoolControlBlock<T> * arrCtrlBlocks;
    static std::map<std::thread::id, size_t> treadIds;
    static size_t numTreads;
    static size_t numTreadsToRegister;
};

template<typename T>
std::map<std::thread::id, size_t> TLSObjectPool<T>::treadIds;
template<typename T>
size_t TLSObjectPool<T>::numTreads;
template<typename T>
size_t TLSObjectPool<T>::numTreadsToRegister;
template<typename T>
TLSObjectPool<T>::TLSPoolControlBlock<T> * TLSObjectPool<T>::arrCtrlBlocks;

/*
    //Sample std::allocator below, can be used like this

    TLSObjectPool<std::_Rb_tree_node <std::pair<const size_t, size_t> > >::setUp(100*1024);
    {
        std::map<size_t, size_t, std::less<size_t>, TLSObjectPoolAllocator<std::pair<const size_t, size_t> > > daMap;
        for (size_t i = 0; i < 102400; ++i)
            daMap[i] = i;
    }
    TLSObjectPool<std::_Rb_tree_node <std::pair<const size_t, size_t> > >::tearDown();

    TLSObjectPool<std::__detail::_Hash_node<std::pair<unsigned long const, unsigned long>, false> >::setUp(100*1024);
    {
        std::unordered_map<size_t, size_t, std::hash<size_t>, std::equal_to<size_t>, TLSObjectPoolAllocator<std::pair<const size_t, size_t> > > daMap;
        for (size_t i = 0; i < 102400; ++i)
            daMap[i] = i;
    }
    TLSObjectPool<std::__detail::_Hash_node<std::pair<unsigned long const, unsigned long>, false> >::tearDown();

*/

template<typename T>
class TLSObjectPoolAllocator : public std::allocator<T>
{
public:
    typedef size_t size_type;
    typedef T* pointer;
    typedef const T* const_pointer;

    template<typename _Tp1>
    struct rebind
    {
        typedef TLSObjectPoolAllocator<_Tp1> other;
    };

    pointer allocate(size_type n, const void *hint = 0)
    {
        if (n == 1)
            return (pointer) TLSObjectPool<T>::acquire();
        else
            return std::allocator<T>::allocate(n, hint);
    }

    void deallocate(pointer p, size_type n)
    {
        if (n == 1)
            return TLSObjectPool<T>::release((T*) p);
        else
            return std::allocator<T>::deallocate(p, n);
    }

    TLSObjectPoolAllocator() throw () :
                    std::allocator<T>()
    {
    }
    TLSObjectPoolAllocator(const TLSObjectPoolAllocator &a) throw () :
                    std::allocator<T>(a)
    {
    }
    template<class U>
    TLSObjectPoolAllocator(const TLSObjectPoolAllocator<U> &a) throw () :
                    std::allocator<T>(a)
    {
    }
    ~TLSObjectPoolAllocator() throw ()
    {
    }
};

#endif /* TLSOBJECTPOOL_H_ */
