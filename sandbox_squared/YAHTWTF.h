#pragma once

#include <vector>
#include <list>
#include <algorithm>

#include "xxhash.h"

#include <immintrin.h>
template <typename K, typename V>
class MyHashMapWTF {
public:

    static const size_t MAGIC_COUNT = 29;

    struct MyBucket
    {
        std::vector<unsigned short> digest;
        std::vector<std::pair<K, V> > elements;
    };

    std::vector<MyBucket> storage;
    size_t item_count;
    size_t bucket_count;

    size_t hash(const K& x) 
    {
        //return std::hash<int>()(x);
        XXH64_hash_t hash = XXH64(&x, sizeof(x), 0); return hash;
    }

    unsigned short fold_hash(size_t x) 
    {
        const unsigned short * pShort = (const unsigned short *) &x;
        const auto res = pShort[0] ^ pShort[1] ^ pShort[2] ^ pShort[3];
        return res;
    }

    MyHashMapWTF(size_t in_item_count) : item_count(in_item_count)
    {
        bucket_count = item_count/MAGIC_COUNT;
        storage.resize(bucket_count);
        for (auto & bucket: storage)
        {
            bucket.digest.reserve(MAGIC_COUNT);
            bucket.elements.reserve(MAGIC_COUNT);
        }
    }

    ~MyHashMapWTF() 
    {
    }

    void put(const K & key, V value) 
    {
        const size_t key_hash = hash(key);
        const unsigned short key_digest = fold_hash(key_hash);
        const size_t bucket_idx = key_hash % bucket_count;
        MyBucket& bucket = storage[bucket_idx];
        const size_t elements_in_bucket = bucket.elements.size();
        for (int i = 0; i < elements_in_bucket; ++i) 
        {
            if (bucket.digest[i] == key_digest) 
	        {
                if (bucket.elements[i].first == key) 
                {
                    bucket.elements[i].second = value;
                    return;
                }
    	    }
        }
        bucket.digest.emplace_back(key_digest);
        bucket.elements.emplace_back(key, value);
    }

    V get(const K & key) {
        const size_t key_hash = hash(key);
        const unsigned short key_digest = fold_hash(key_hash);
        const size_t bucket_idx = key_hash % bucket_count;
        MyBucket& bucket = storage[bucket_idx];
        const size_t elements_in_bucket = bucket.elements.size();
        for (int i = 0; i < elements_in_bucket; ++i) 
        {
            if (bucket.digest[i] == key_digest) 
	        {
                if (bucket.elements[i].first == key)  
                {
                    return bucket.elements[i].second;
                }
    	    }
        }
        return V();
    }

    void remove(const K & key) {
        const size_t key_hash = hash(key);
        const unsigned short key_digest = fold_hash(key_hash);
        const size_t bucket_idx = key_hash % bucket_count;
        MyBucket& bucket = storage[bucket_idx];
        const size_t elements_in_bucket = bucket.elements.size();
        for (int i = 0; i < elements_in_bucket; ++i) 
        {
            if (bucket.digest[i] == key_digest) 
	        {
                if (bucket.elements[i].first == key) 
                {
                    if ((elements_in_bucket > 1) && (i != elements_in_bucket-1)) 
                    {
                        std::swap(bucket.digest[i], bucket.digest[elements_in_bucket-1]);
                        std::swap(bucket.elements[i], bucket.elements[elements_in_bucket-1]);
                    }
                    bucket.digest.resize(elements_in_bucket-1);
                    bucket.elements.resize(elements_in_bucket-1);
                    return;
                }
    	    }
        }
    }
};
