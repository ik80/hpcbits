#ifndef SPARSEARRAY_H_
#define SPARSEARRAY_H_

#include <memory>
#include <vector>

template<typename T, unsigned long long HOLY_GRAIL_SIZE = 64>
class SparseArray
{
private:

    static const unsigned long long BITS_PER_BYTE = 8;
    static const unsigned long long CACHE_LINE_SIZE = 64;

    struct SparseArrayBucket
    {
        T*              elements; //8
        unsigned char   bitmap[HOLY_GRAIL_SIZE/BITS_PER_BYTE]; //8 with default HOLY_GRAIL_SIZE

        inline void bitmapSet(const unsigned long long pos)
        {
            const unsigned long long ullIdx = pos / (BITS_PER_BYTE*(sizeof(unsigned long long)));
            const unsigned long long ullOffset = pos % (BITS_PER_BYTE*(sizeof(unsigned long long)));
            unsigned long long * const pUll = (unsigned long long * const) &(bitmap[ullIdx*sizeof(unsigned long long)]);
            (*pUll) |= (1ULL << ullOffset);
        }
        inline void bitmapClear(const unsigned long long pos)
        {
            const unsigned long long ullIdx = pos / (BITS_PER_BYTE*(sizeof(unsigned long long)));
            const unsigned long long ullOffset = pos % (BITS_PER_BYTE*(sizeof(unsigned long long)));
            unsigned long long * const pUll = (unsigned long long * const) &(bitmap[ullIdx*sizeof(unsigned long long)]);
            (*pUll) &= ~(1ULL << ullOffset);
        }
        inline bool bitmapTest(const unsigned long long pos) const
        {
            const unsigned long long ullIdx = pos / (BITS_PER_BYTE*(sizeof(unsigned long long)));
            const unsigned long long ullOffset = pos % (BITS_PER_BYTE*(sizeof(unsigned long long)));
            unsigned long long * const pUll = (unsigned long long * const) &(bitmap[ullIdx*sizeof(unsigned long long)]);
            return (*pUll) & (1ULL << ullOffset);
        }
    }__attribute__((aligned(1), packed));


    SparseArrayBucket*  buckets; //8
    unsigned long long  bucketsLen; //8
public:
    unsigned long long  maxElements; //8
private:
    char padding0[40]; // padding to cacheline

    unsigned long long googlerank(const unsigned char *bm, unsigned long long pos);
    unsigned long long bits_in_char(unsigned char c);


public:
    SparseArray();
    SparseArray(unsigned long long inMaxElements);
    ~SparseArray();
    SparseArray(const SparseArray& other);
    SparseArray& operator=(const SparseArray& other);

    bool add(unsigned long long idx, const T & inValue);
    bool get(unsigned long long idx, T & outValue);
    bool set(unsigned long long idx, const T & inValue);
    bool remove(unsigned long long idx);
    bool exists(unsigned long long idx);

    void swap(SparseArray& other);

}__attribute__((aligned(CACHE_LINE_SIZE)));

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
unsigned long long SparseArray<T, HOLY_GRAIL_SIZE>::bits_in_char(unsigned char c)
{
  // We could make these ints.  The tradeoff is size (eg does it overwhelm
  // the cache?) vs efficiency in referencing sub-word-sized array buckets.
  static __thread const char bits_in[256] =
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
  return bits_in[c];
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
unsigned long long SparseArray<T, HOLY_GRAIL_SIZE>::googlerank(const unsigned char *bm, unsigned long long pos)
{
    unsigned long long retval = 0;
  for (; pos > 8ULL; pos -= 8ULL)
      retval += bits_in_char(*bm++);
  return retval + bits_in_char(*bm & ((1ULL << pos)-1ULL));
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
void SparseArray<T, HOLY_GRAIL_SIZE>::swap(SparseArray<T, HOLY_GRAIL_SIZE>& other)
{
    std::swap(buckets, other.buckets);
    std::swap(bucketsLen, other.bucketsLen);
    std::swap(maxElements, other.maxElements);
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
SparseArray<T, HOLY_GRAIL_SIZE>::SparseArray() : buckets(0), bucketsLen(0), maxElements(0)
{
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
SparseArray<T, HOLY_GRAIL_SIZE>::SparseArray(unsigned long long inMaxElements) :
    buckets(0),
    bucketsLen(inMaxElements / HOLY_GRAIL_SIZE + (inMaxElements % HOLY_GRAIL_SIZE ? 1ULL : 0ULL)),
    maxElements(inMaxElements)
{
    buckets = (SparseArrayBucket*) std::malloc(bucketsLen*sizeof(SparseArrayBucket));
    memset(buckets, 0, bucketsLen*sizeof(SparseArrayBucket));
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
SparseArray<T, HOLY_GRAIL_SIZE>::~SparseArray()
{
    for(unsigned long long bucketPos = 0; bucketPos < bucketsLen; ++bucketPos)
    {
        SparseArrayBucket * bucket = &(buckets[bucketPos]);
        if (bucket)
        {
            unsigned long long count = googlerank((const unsigned char*) bucket->bitmap, HOLY_GRAIL_SIZE/BITS_PER_BYTE);
            for(unsigned long long j = 0; j < count; ++j)
            {
                bucket->elements[j].~T();
            }
            std::free(bucket->elements);
        }
    }
    std::free(buckets);
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
SparseArray<T, HOLY_GRAIL_SIZE>::SparseArray(const SparseArray<T, HOLY_GRAIL_SIZE>& other) :
    buckets(0),
    bucketsLen(other.bucketsLen),
    maxElements(other.maxElements)
{
    buckets = (SparseArrayBucket*) std::malloc(bucketsLen*sizeof(SparseArrayBucket));
    memset(buckets, 0, bucketsLen*sizeof(SparseArrayBucket*));
    for(unsigned long long bucketPos = 0; bucketPos < other.bucketsLen; ++bucketPos)
    {
        T * bucket = &(buckets[bucketPos]);
        *bucket = {0};
        T * otherbucket = &(other.buckets[bucketPos]);
        if (otherbucket)
        {
            unsigned long long count = googlerank((const unsigned char*) otherbucket->bitmap, HOLY_GRAIL_SIZE/BITS_PER_BYTE);
            bucket->elements = (T*) std::malloc(count*sizeof(T));
            for(unsigned long long j = 0; j < count; ++j)
            {
                new (&bucket->elements[j]) T(otherbucket->elements[j]);
            }
        }
    }
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
SparseArray<T, HOLY_GRAIL_SIZE>& SparseArray<T, HOLY_GRAIL_SIZE>::operator=(const SparseArray<T, HOLY_GRAIL_SIZE>& other)
{
    if(&other != this)
    {
        SparseArray<T, HOLY_GRAIL_SIZE> tmpArray(other);
        swap(tmpArray);
    }
    return *this;
}


template<typename T, unsigned long long HOLY_GRAIL_SIZE>
bool SparseArray<T, HOLY_GRAIL_SIZE>::get(unsigned long long idx, T & outValue)
{
    const unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    const unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseArrayBucket * const bucket = &(buckets[bucketPos]);
    if(bucket->bitmapTest(bucketOffset))
    {
        const unsigned long long rank = googlerank((const unsigned char*) bucket->bitmap, bucketOffset + 1);
        outValue = bucket->elements[rank-1];
        return true;
    }
    return false;
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
bool SparseArray<T, HOLY_GRAIL_SIZE>::set(unsigned long long idx, const T & inValue)
{
    const unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    const unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseArrayBucket * const bucket = &(buckets[bucketPos]);
    if(bucket->bitmapTest(bucketOffset))
    {
        const unsigned long long rank = googlerank((const unsigned char*) bucket->bitmap, bucketOffset + 1);
        bucket->elements[rank-1] = inValue;
        return true;
    }
    return false;
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
bool SparseArray<T, HOLY_GRAIL_SIZE>::add(unsigned long long idx, const T & inValue)
{
    const unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    const unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseArrayBucket * const bucket = &(buckets[bucketPos]);
    if(bucket->elements == 0)
    {
        bucket->elements = (T *) std::malloc(sizeof(T));
        new (&(bucket->elements[0])) T(inValue);
        bucket->bitmapSet(bucketOffset);
        return true;
    }
    else if (!bucket->bitmapTest(bucketOffset))
    {
        const unsigned long long count = googlerank((const unsigned char*) bucket->bitmap, HOLY_GRAIL_SIZE);
        const unsigned long long rank = googlerank((const unsigned char*) bucket->bitmap, bucketOffset + 1);
        bucket->elements = (T*) std::realloc(bucket->elements, (count + 1) * sizeof(T));
        if(rank < count)
            memmove(bucket->elements + rank + 1, bucket->elements + rank, (count - rank) * sizeof(T));

        new (&bucket->elements[rank]) T(inValue);
        bucket->bitmapSet(bucketOffset);
        return true;
    }
    return false;
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
bool SparseArray<T, HOLY_GRAIL_SIZE>::remove(unsigned long long idx)
{
    const unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    const unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseArrayBucket * const bucket = &(buckets[bucketPos]);

    if (bucket->bitmapTest(bucketOffset))
    {
        const unsigned long long count = googlerank((const unsigned char*) bucket->bitmap, HOLY_GRAIL_SIZE);
        if (count == 1)
        {
            std::free(bucket->elements);
            bucket->elements = 0;
        }
        else
        {
            const unsigned long long rank = googlerank((const unsigned char*) bucket->bitmap, bucketOffset + 1);
            if(rank < count)
                memmove(bucket->elements + rank - 1, bucket->elements + rank, (count - rank) * sizeof(T));
            bucket->elements = (T*) std::realloc(bucket->elements, (count - 1) * sizeof(T));
        }
        bucket->bitmapClear(bucketOffset);
        return true;
    }
    return false;
}

template<typename T, unsigned long long HOLY_GRAIL_SIZE>
bool SparseArray<T, HOLY_GRAIL_SIZE>::exists(unsigned long long idx)
{
    const unsigned long long bucketPos = idx / HOLY_GRAIL_SIZE;
    const unsigned long long bucketOffset = idx % HOLY_GRAIL_SIZE;
    SparseArrayBucket * const bucket = &(buckets[bucketPos]);
    if(bucket->elements && bucket->bitmapTest(bucketOffset))
    {
        return true;
    }
    return false;
}
#endif /* SPARSEARRAY_H_ */
