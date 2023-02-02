/*
 * LFSparseHashTableUtil.h
 *
 *  Created on: Dec 18, 2016
 *      Author: kalujny
 */

#ifndef LFSPARSEHASHTABLEUTIL_H_
#define LFSPARSEHASHTABLEUTIL_H_

#include <vector>
#include <boost/thread.hpp>

#include "LFSparseHashTable.h"

// TODO: change the code to work with fd's instead of filenames so it can work with both sockets and files, pack bucket info before data
// TODO: redo *Raw methods into threaded implementation

template<typename K, typename V, class HashFunc = std::hash<K> >
class LFSparseHashTableUtil
{
public:
    typedef LFSparseHashTable<K, V, HashFunc> LFHT;
    typedef typename LFHT::SparseBucket LFHTSB;
    typedef typename LFHT::SparseBucketElement LFHTSBE;

    // ONLY FOR POD TYPES
    static void saveRaw(LFSparseHashTable<K, V, HashFunc> * pTable, const char * fileName)
    {
        FILE * pFile = fopen(fileName, "wb+");
        fwrite(pTable, sizeof(LFHT), 1, pFile);
        fwrite(pTable->buckets, sizeof(LFHTSB), (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)), pFile);
        for(unsigned long long packBucketPos = 0;
                        packBucketPos < pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++packBucketPos)
        {
            LFHTSB * bucket = &(pTable->buckets[packBucketPos]);
            unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
            LFHTSBE * bucketElements = bucket->elements;
            fwrite(bucketElements, sizeof(LFHTSBE), count, pFile);
        }
        fclose(pFile);
    }

    // ONLY FOR POD TYPES
    static void loadRaw(LFSparseHashTable<K, V, HashFunc> * pTable, const char * fileName)
    {
        for(unsigned long long bucketPos = 0; bucketPos < pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
                        ++bucketPos)
        {
            LFHTSB * bucket = &(pTable->buckets[bucketPos]);
            unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
            LFHTSBE * bucketElements = bucket->elements;
            for(unsigned long long j = 0; j < count; ++j)
            {
                bucketElements[j].~SparseBucketElement();
            }
            std::free(bucket->elements);
        }
        std::free(pTable->buckets);

        FILE * pFile = fopen(fileName, "rb");
        char metaBuffer[sizeof(LFHT)];
        fread(metaBuffer, sizeof(LFHT), 1, pFile);
        unsigned long long savedMaxElements = ((LFHT*) metaBuffer)->maxElements;
        if(pTable->maxElements == savedMaxElements)
        {
            pTable->maxLoadFactor = ((LFHT*) metaBuffer)->maxLoadFactor;
            pTable->minLoadFactor = ((LFHT*) metaBuffer)->minLoadFactor;

            pTable->buckets = (LFHTSB*) std::malloc(
                            (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));
            memset(pTable->buckets, 0, (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));
            fread(pTable->buckets, sizeof(LFHTSB), (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)),
                  pFile);

            for(unsigned long long packBucketPos = 0;
                            packBucketPos < pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
                            ++packBucketPos)
            {
                LFHTSB * bucket = &(pTable->buckets[packBucketPos]);
                bucket->elementLocks = 0;
                unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                bucket->elements = (LFHTSBE *) std::malloc(count * sizeof(LFHTSBE));
                fread(bucket->elements, sizeof(LFHTSBE), count, pFile);
            }
        }
        else
        {
            pTable->buckets = (LFHTSB*) std::malloc(
                            (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));
            memset(pTable->buckets, 0, (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));

            LFHTSB * pSavedBuckets = (LFHTSB*) std::malloc(
                            (savedMaxElements / LFHT::HOLY_GRAIL_SIZE + (savedMaxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));
            memset(pSavedBuckets, 0, (savedMaxElements / LFHT::HOLY_GRAIL_SIZE + (savedMaxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));
            fread(pSavedBuckets, sizeof(LFHTSB), (savedMaxElements / LFHT::HOLY_GRAIL_SIZE + (savedMaxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)), pFile);
            for(unsigned long long packBucketPos = 0;
                            packBucketPos < savedMaxElements / LFHT::HOLY_GRAIL_SIZE + (savedMaxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
                            ++packBucketPos)
            {
                LFHTSB * savedbucket = &(pSavedBuckets[packBucketPos]);
                savedbucket->elementLocks = 0;
                unsigned long long count = googlerank((const unsigned char*) &(savedbucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                LFHTSBE * pSavedBucketElements = (LFHTSBE *) std::malloc(count * sizeof(LFHTSBE));
                fread(pSavedBucketElements, sizeof(LFHTSBE), count, pFile);

                // calc all hashes and set bits in new hash
                for(unsigned long long eidx = 0; eidx < count; ++eidx)
                {
                    K inKey = pSavedBucketElements[eidx].key;
                    unsigned long long idx = pTable->hasherFunc(inKey) % pTable->maxElements; // TODO: seed
                    unsigned long long bucketPos = idx / LFHT::HOLY_GRAIL_SIZE;
                    unsigned long long bucketOffset = idx % LFHT::HOLY_GRAIL_SIZE;
                    LFHTSB * bucket = &(pTable->buckets[bucketPos]);
                    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                    if(bucket->elementBitmap == 0)
                    {
                        bucket->bitmapSet(bucketOffset);
                    }
                    else
                    {
                        bool stepBack = false; // to avoid extra rank calculation later
                        unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
                        while(true) // linear probing
                        {
                            bool elementExists = bucket->bitmapTest(bucketOffset);
                            if(elementExists)
                            {
                                stepBack = true;
                                ++rank;
                                ++bucketOffset;
                                idx = (idx + 1) % pTable->maxElements;
                                if(bucketOffset == LFHT::HOLY_GRAIL_SIZE)
                                {
                                    rank = 1;
                                    bucketPos = idx / LFHT::HOLY_GRAIL_SIZE;
                                    bucketOffset = idx % LFHT::HOLY_GRAIL_SIZE;
                                    bucket = &(pTable->buckets[bucketPos]);
                                    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                                    if(bucket->elementBitmap == 0)
                                    {
                                        bucket->bitmapSet(bucketOffset);
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                if(stepBack)
                                    --rank;
                                bucket->bitmapSet(bucketOffset);
                                break;
                            }
                        }
                    }
                }
                std::free(pSavedBucketElements);
            }
            // by now we have all bits set in all pTable->buckets as if we had loaded the pTable->
            // alloc memory according to bucket counts and process file again to "insert allocated" all elements
            for(unsigned long long bucketPos = 0;
                            bucketPos < pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++bucketPos)
            {
                LFHTSB * bucket = &(pTable->buckets[bucketPos]);
                unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                bucket->elements = (LFHTSBE *) std::malloc(sizeof(LFHTSBE) * count);
                bucket->elementLocks = 0ULL;
                bucket->elementBitmap = 0ULL;
            }

            // rewind the file
            fseek(pFile,
                  sizeof(LFHT) + sizeof(LFHTSB) * (savedMaxElements / LFHT::HOLY_GRAIL_SIZE + (savedMaxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)),
                  SEEK_SET);

            // insert allocated
            for(unsigned long long packBucketPos = 0;
                            packBucketPos < savedMaxElements / LFHT::HOLY_GRAIL_SIZE + (savedMaxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
                            ++packBucketPos)
            {
                LFHTSB * savedBucket = &(pSavedBuckets[packBucketPos]);
                savedBucket->elementLocks = 0;
                unsigned long long savedcount = googlerank((const unsigned char*) &(savedBucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                LFHTSBE * pSavedBucketElements = (LFHTSBE *) std::malloc(savedcount * sizeof(LFHTSBE));
                fread(pSavedBucketElements, sizeof(LFHTSBE), savedcount, pFile);

                // every element should be inserted allocated into first 0 of pBucketsCopy
                for(unsigned long long sidx = 0; sidx < savedcount; ++sidx)
                {
                    K inKey = pSavedBucketElements[sidx].key;
                    V inValue = pSavedBucketElements[sidx].value;
                    unsigned long long idx = pTable->hasherFunc(inKey) % pTable->maxElements;
                    unsigned long long bucketPos = idx / LFHT::HOLY_GRAIL_SIZE;
                    unsigned long long bucketOffset = idx % LFHT::HOLY_GRAIL_SIZE;
                    LFHTSB * bucket = &(pTable->buckets[bucketPos]);
                    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                    LFHTSBE * bucketElements = bucket->elements;
                    if(bucket->elementBitmap == 0)
                    {
                        bucketElements = bucket->elements;
                        new (&(bucketElements[0].key)) K(std::move(inKey));
                        new (&(bucketElements[0].value)) V(std::move(inValue));
                        bucket->bitmapSet(bucketOffset);
                        continue;
                    }
                    else
                    {
                        bool stepBack = false; // to avoid extra rank calculation later
                        unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
                        while(true) // linear probing
                        {
                            bool elementExists = bucket->bitmapTest(bucketOffset);
                            if(elementExists)
                            {
                                stepBack = true;
                                ++rank;
                                ++bucketOffset;
                                idx = (idx + 1) % pTable->maxElements;
                                if(bucketOffset == LFHT::HOLY_GRAIL_SIZE)
                                {
                                    rank = 1;
                                    bucketPos = idx / LFHT::HOLY_GRAIL_SIZE;
                                    bucketOffset = idx % LFHT::HOLY_GRAIL_SIZE;
                                    bucket = &(pTable->buckets[bucketPos]);
                                    __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                                    bucketElements = bucket->elements;
                                    if(bucket->elementBitmap == 0)
                                    {
                                        new (&(bucketElements[0].key)) K(std::move(inKey));
                                        new (&(bucketElements[0].value)) V(std::move(inValue));
                                        bucket->bitmapSet(bucketOffset);
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                if(stepBack)
                                    --rank;
                                unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                                if(rank < count)
                                    memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(LFHTSBE));
                                new (&(bucketElements[rank].key)) K(std::move(inKey));
                                new (&(bucketElements[rank].value)) V(std::move(inValue));
                                bucket->bitmapSet(bucketOffset);
                                break;
                            }
                        }
                    }
                }
                std::free(pSavedBucketElements);
            }
            std::free(pSavedBuckets);
        }
        fclose(pFile);
    }

    static void save(LFSparseHashTable<K, V, HashFunc> * pTable, const char * fileName, std::function<void(LFHTSBE&, char*)> itemSaveFunc, std::function<size_t(LFHTSBE&)> itemSizeFunc)
    {
        FILE * pFile = fopen(fileName, "wb+");
        fwrite(pTable, sizeof(LFHT), 1, pFile);
        fwrite(pTable->buckets, sizeof(LFHTSB), (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)), pFile);
        std::vector<char> buffer;
        size_t numBuckets = pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
        size_t numStreams = boost::thread::hardware_concurrency();
        size_t bucketsPerStream = numBuckets / numStreams + 1;
        std::vector<std::shared_ptr<boost::thread> > savers(numStreams);
        std::string itemDataFileNames;
        for (size_t idxStream = 0; idxStream < numStreams; ++idxStream)
        {
            static const size_t MAX_PATH = 1024;
            char fileNameBuf[MAX_PATH];
            snprintf(fileNameBuf,MAX_PATH, "%s.data.%lu.%lu", fileName, idxStream*bucketsPerStream, bucketsPerStream*(idxStream+1) > numBuckets ? numBuckets : bucketsPerStream*(idxStream+1));
            itemDataFileNames += fileNameBuf;
            if (idxStream != numStreams - 1)
                itemDataFileNames += std::string(";");
            savers[idxStream].reset(new boost::thread(saveThreadProc, pTable, std::string(fileNameBuf), idxStream*bucketsPerStream, bucketsPerStream*(idxStream+1) > numBuckets ? numBuckets : bucketsPerStream*(idxStream+1), itemSaveFunc, itemSizeFunc));
        }
        fwrite(itemDataFileNames.c_str(), itemDataFileNames.size(), 1, pFile);
        fclose(pFile);
        for (size_t idxStream = 0; idxStream < numStreams; ++idxStream)
        {
            savers[idxStream]->join();
        }
    }

    static void load(LFSparseHashTable<K, V, HashFunc> * pTable, const char * fileName, std::function<void(LFHTSBE&, char*)> itemLoadFunc)
    {
        for(unsigned long long bucketPos = 0; bucketPos < pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL);
                        ++bucketPos)
        {
            LFHTSB * bucket = &(pTable->buckets[bucketPos]);
            unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
            LFHTSBE * bucketElements = bucket->elements;
            for(unsigned long long j = 0; j < count; ++j)
            {
                bucketElements[j].~SparseBucketElement();
            }
            std::free(bucket->elements);
        }
        std::free(pTable->buckets);

        FILE * pFile = fopen(fileName, "rb");
        char metaBuffer[sizeof(LFHT)];
        fread(metaBuffer, sizeof(LFHT), 1, pFile);
        unsigned long long savedMaxElements = ((LFHT*) metaBuffer)->maxElements;
        if(pTable->maxElements != savedMaxElements)
        {
            pTable->buckets = (LFHTSB*) std::malloc(
                            (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));
            memset(pTable->buckets, 0, (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));

            // Seek past saved buckets
            fseek(pFile, sizeof(LFHTSB)*(savedMaxElements / LFHT::HOLY_GRAIL_SIZE + (savedMaxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)), SEEK_CUR);
            static const size_t MAX_FILENAMES_LEN = 32768;
            char fileNamesBuf[MAX_FILENAMES_LEN];
            std::vector<std::string> fileNames;
            fileNamesBuf[fread(fileNamesBuf, 1, MAX_FILENAMES_LEN, pFile)] = 0;
            char* pch = strtok(fileNamesBuf,";");
            while (pch != NULL)
            {
              fileNames.push_back(pch);
              pch = strtok (NULL, ";");
            }
            fclose(pFile);
            std::vector<std::shared_ptr<boost::thread> > loaders(fileNames.size());
            for (size_t idxStream = 0; idxStream < fileNames.size(); ++idxStream)
            {
                loaders[idxStream].reset(new boost::thread(loadThreadProc, pTable, fileNames[idxStream], itemLoadFunc));
            }
            for (size_t idxStream = 0; idxStream < loaders.size(); ++idxStream)
            {
                loaders[idxStream]->join();
            }
        }
        else 
        {
            pTable->buckets = (LFHTSB*) std::malloc(
                            (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)) * sizeof(LFHTSB));
            fread(pTable->buckets, sizeof(LFHTSB), (pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL)), pFile);
            std::vector<unsigned char> freeSlots;
            // by now we have all bits set in all pTable->buckets as if we had loaded the pTable->
            // alloc memory according to bucket counts and process file again to "insert allocated" all elements
            for(unsigned long long bucketPos = 0;
                            bucketPos < pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++bucketPos)
            {
                LFHTSB * bucket = &(pTable->buckets[bucketPos]);
                unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                bucket->elements = (LFHTSBE *) std::malloc(sizeof(LFHTSBE) * count);
                freeSlots.push_back(count);
                bucket->elementLocks = 0ULL;
                bucket->elementBitmap = 0ULL;
            }

            static const size_t MAX_FILENAMES_LEN = 32768;
            char fileNamesBuf[MAX_FILENAMES_LEN];
            std::vector<std::string> fileNames;
            fileNamesBuf[fread(fileNamesBuf, 1, MAX_FILENAMES_LEN, pFile)] = 0;
            char* pch = strtok(fileNamesBuf,";");
            while (pch != NULL)
            {
              fileNames.push_back(pch);
              pch = strtok (NULL, ";");
            }
            fclose(pFile);

            std::vector<std::shared_ptr<boost::thread> > loaders(fileNames.size());
            std::vector<std::vector< LFHTSBE > > misfits(loaders.size());
            for (size_t idxStream = 0; idxStream < fileNames.size(); ++idxStream)
            {
                loaders[idxStream].reset(new boost::thread(loadThreadProcComplex, pTable, fileNames[idxStream], itemLoadFunc, &(misfits[idxStream]), &freeSlots));
            }
            for (size_t idxStream = 0; idxStream < loaders.size(); ++idxStream)
            {
                loaders[idxStream]->join();
            }
            for(unsigned long long bucketPos = 0;
                            bucketPos < pTable->maxElements / LFHT::HOLY_GRAIL_SIZE + (pTable->maxElements % LFHT::HOLY_GRAIL_SIZE ? 1ULL : 0ULL); ++bucketPos)
            {
                if (freeSlots[bucketPos])
                {
                    LFHTSB * bucket = &(pTable->buckets[bucketPos]);
                    unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                    bucket->elements = (LFHTSBE *) std::realloc(bucket->elements, count * sizeof(LFHTSBE));
                }
            }
            for (size_t idxStream = 0; idxStream < loaders.size(); ++idxStream)
            {
                for(unsigned long long misfitPos = 0; misfitPos < misfits[idxStream].size(); ++ misfitPos)
                {
                    pTable->insert(misfits[idxStream][misfitPos].key, misfits[idxStream][misfitPos].value);
                }
            }
        }
    }
private:
    static void saveThreadProc(LFSparseHashTable<K, V, HashFunc> * pTable, std::string fileName, size_t startIdx, size_t endIdx, std::function<void(LFHTSBE&, char*)> itemSaveFunc, std::function<size_t(LFHTSBE&)> itemSizeFunc)
    {
        FILE * pFile = fopen(fileName.c_str(), "wb+");
        for(unsigned long long packBucketPos = startIdx; packBucketPos < endIdx; ++packBucketPos)
        {
            std::vector<char> buffer;
            LFHTSB * bucket = &(pTable->buckets[packBucketPos]);
            unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
            LFHTSBE * bucketElements = bucket->elements;
            for (size_t itemIdx = 0; itemIdx < count; ++itemIdx)
            {
                size_t curItemSize = itemSizeFunc(bucketElements[itemIdx]);
                buffer.reserve(sizeof(size_t) + curItemSize);
                memcpy(&buffer[0],&curItemSize, sizeof(size_t));
                itemSaveFunc(bucketElements[itemIdx], &buffer[sizeof(size_t)]);
                fwrite(&buffer[0], 1, sizeof(size_t) + curItemSize, pFile);
            }
        }
        fclose(pFile);
    }

    static void loadThreadProc(LFSparseHashTable<K, V, HashFunc> * pTable, std::string fileName, std::function<void(LFHTSBE&, char*)> itemLoadFunc)
    {
        enum READER_STATE
        {
            READING_SIZE,
            READING_ITEM
        };

        FILE * pFile = fopen(fileName.c_str(), "rb");

        size_t sizeBytesRead = 0;
        size_t itemBytesRead = 0;

        READER_STATE readerState = READING_SIZE;

        std::vector<char> buffer(1024*1024);

        size_t readIdx = 0;
        size_t bytesRead = 0;
        size_t readPos = 0;

        size_t nextItemSavedSize = 0;

        while(true)
        {
            bool largeItem = false;
            bytesRead = fread(&buffer[readPos], 1, buffer.capacity() - readPos, pFile) + readPos;
            readPos = 0;
            if (!bytesRead)
                break; // TODO: anything left behind is bad news
            readIdx = 0;
            while(readIdx < bytesRead && !largeItem)
            {
                switch (readerState)
                {
                    case READING_SIZE:
                    {
                        if (bytesRead - readIdx >= (sizeof(size_t) - sizeBytesRead))
                        {
                            memcpy(((char*)&nextItemSavedSize) + sizeBytesRead, &buffer[readIdx], sizeof(size_t) - sizeBytesRead);
                            readIdx += sizeof(size_t) - sizeBytesRead;
                            readerState = READING_ITEM;
                            itemBytesRead = 0;
                            sizeBytesRead = 0;
                        }
                        else
                        {
                            memcpy(((char*)&nextItemSavedSize) + sizeBytesRead, &buffer[readIdx], bytesRead - readIdx);
                            sizeBytesRead += bytesRead - readIdx;
                            readIdx += bytesRead - readIdx;
                        }
                        if (readerState == READING_ITEM && nextItemSavedSize >= (buffer.capacity() + sizeof(size_t)))
                        {
                            buffer.resize(buffer.capacity() + nextItemSavedSize + sizeof(size_t));
                            largeItem = true;
                        }
                    }
                    break;
                    case READING_ITEM:
                    {
                        if (bytesRead - readIdx >= (nextItemSavedSize - itemBytesRead))
                        {
                            LFHTSBE nextItem;
                            itemLoadFunc(nextItem, &buffer[readIdx]);
                            pTable->insert(nextItem.key, nextItem.value);
                            readIdx += nextItemSavedSize - itemBytesRead;
                            readerState = READING_SIZE;
                        }
                        else
                        {
                            memmove(&buffer[0], &buffer[readIdx - itemBytesRead], bytesRead - readIdx + itemBytesRead);
                            readPos = bytesRead - readIdx;
                            readIdx += bytesRead - readIdx;
                            itemBytesRead = 0;
                        }
                    }
                    break;
                    default:
                        abort();
                    break;
                }
            }
        }
        fclose(pFile);
    }

    static void loadThreadProcComplex(LFSparseHashTable<K, V, HashFunc> * pTable, std::string fileName, std::function<void(LFHTSBE&, char*)> itemLoadFunc, std::vector<LFHTSBE>* misfits, std::vector<unsigned char>* freeSlots)
    {
        enum READER_STATE
        {
            READING_SIZE,
            READING_ITEM
        };

        size_t endBucketPos = atoi(fileName.substr(fileName.rfind('.')+1).c_str());
        size_t beginBucketPos = atoi(fileName.substr(fileName.rfind('.', fileName.rfind('.')-1)+1, fileName.rfind('.') - (fileName.rfind('.', fileName.rfind('.')-1)+1)).c_str());

        FILE * pFile = fopen(fileName.c_str(), "rb");

        size_t sizeBytesRead = 0;
        size_t itemBytesRead = 0;

        READER_STATE readerState = READING_SIZE;

        std::vector<char> buffer(1024*1024);

        size_t readIdx = 0;
        size_t bytesRead = 0;
        size_t readPos = 0;

        size_t nextItemSavedSize = 0;

        while(true)
        {
            bool largeItem = false;
            bytesRead = fread(&buffer[readPos], 1, buffer.capacity() - readPos, pFile) + readPos;
            readPos = 0;
            if (!bytesRead)
                break; // TODO: anything left behind is bad news
            readIdx = 0;
            while(readIdx < bytesRead && !largeItem)
            {
                switch (readerState)
                {
                    case READING_SIZE:
                    {
                        if (bytesRead - readIdx >= (sizeof(size_t) - sizeBytesRead))
                        {
                            memcpy(((char*)&nextItemSavedSize) + sizeBytesRead, &buffer[readIdx], sizeof(size_t) - sizeBytesRead);
                            readIdx += sizeof(size_t) - sizeBytesRead;
                            readerState = READING_ITEM;
                            sizeBytesRead = 0;
                            itemBytesRead = 0;
                        }
                        else
                        {
                            memcpy(((char*)&nextItemSavedSize) + sizeBytesRead, &buffer[readIdx], bytesRead - readIdx);
                            sizeBytesRead += bytesRead - readIdx;
                            readIdx += bytesRead - readIdx;
                        }
                        if (readerState == READING_ITEM && nextItemSavedSize >= (buffer.capacity() + sizeof(size_t)))
                        {
                            buffer.resize(buffer.capacity() + nextItemSavedSize + sizeof(size_t));
                            largeItem = true;
                        }

                    }
                    break;
                    case READING_ITEM:
                    {
                        if (bytesRead - readIdx >= (nextItemSavedSize - itemBytesRead))
                        {
                            LFHTSBE nextItem;
                            itemLoadFunc(nextItem, &buffer[readIdx]);

                            // do da elephant
                            unsigned long long idx = pTable->hasherFunc(nextItem.key) % pTable->maxElements; // TODO: seed
                            unsigned long long bucketPos = idx / LFHT::HOLY_GRAIL_SIZE;

                            if (bucketPos >= beginBucketPos && bucketPos < endBucketPos)
                            {
                                bool inserted = false;
                                unsigned long long bucketOffset = idx % LFHT::HOLY_GRAIL_SIZE;
                                LFHTSB * bucket = &(pTable->buckets[bucketPos]);
                                __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                                LFHTSBE * bucketElements = bucket->elements;
                                if(bucket->elementBitmap == 0 && (*freeSlots)[bucketPos])
                                {
                                    bucketElements = bucket->elements;
                                    new (&(bucketElements[0].key)) K(nextItem.key);
                                    new (&(bucketElements[0].value)) V(nextItem.value);
                                    --((*freeSlots)[bucketPos]);
                                    inserted = true;
                                    bucket->bitmapSet(bucketOffset);
                                }
                                else
                                {
                                    bool stepBack = false; // to avoid extra rank calculation later
                                    unsigned long long rank = googlerank((const unsigned char*) &(bucket->elementBitmap), bucketOffset + 1);
                                    while(true) // linear probing
                                    {
                                        bool elementExists = bucket->bitmapTest(bucketOffset);
                                        if(elementExists)
                                        {
                                            stepBack = true;
                                            ++rank;
                                            ++bucketOffset;
                                            idx = (idx + 1) % pTable->maxElements;
                                            if(bucketOffset == LFHT::HOLY_GRAIL_SIZE)
                                            {
                                                rank = 1;
                                                bucketPos = idx / LFHT::HOLY_GRAIL_SIZE;
                                                bucketOffset = idx % LFHT::HOLY_GRAIL_SIZE;
                                                bucket = &(pTable->buckets[bucketPos]);
                                                __builtin_prefetch(bucket, 1, 3 /* _MM_HINT_T0 */);
                                                bucketElements = bucket->elements;
                                                if(bucket->elementBitmap == 0 && (*freeSlots)[bucketPos])
                                                {
                                                    new (&(bucketElements[0].key)) K(nextItem.key);
                                                    new (&(bucketElements[0].value)) V(nextItem.value);
                                                    --((*freeSlots)[bucketPos]);
                                                    inserted = true;
                                                    bucket->bitmapSet(bucketOffset);
                                                    break;
                                                }
                                            }
                                        }
                                        else if ((*freeSlots)[bucketPos])
                                        {
                                            if(stepBack)
                                                --rank;
                                            unsigned long long count = googlerank((const unsigned char*) &(bucket->elementBitmap), LFHT::HOLY_GRAIL_SIZE);
                                            if(rank < count)
                                                memmove(bucketElements + rank + 1, bucketElements + rank, (count - rank) * sizeof(LFHTSBE));
                                            new (&(bucketElements[rank].key)) K(nextItem.key);
                                            new (&(bucketElements[rank].value)) V(nextItem.value);
                                            --((*freeSlots)[bucketPos]);
                                            inserted = true;
                                            bucket->bitmapSet(bucketOffset);
                                            break;
                                        }
                                    }
                                }
                                if (!inserted)
                                    (*misfits).push_back(nextItem);
                            }
                            else
                                (*misfits).push_back(nextItem);

                            readIdx += nextItemSavedSize - itemBytesRead;
                            readerState = READING_SIZE;
                        }
                        else
                        {
                            memmove(&buffer[0], &buffer[readIdx - itemBytesRead], bytesRead - readIdx + itemBytesRead);
                            readPos = bytesRead - readIdx;
                            readIdx += bytesRead - readIdx;
                            itemBytesRead = 0;
                        }
                    }
                    break;
                    default:
                        abort();
                    break;
                }
            }
        }
        fclose(pFile);
    }
};

#endif /* LFSPARSEHASHTABLEUTIL_H_ */
