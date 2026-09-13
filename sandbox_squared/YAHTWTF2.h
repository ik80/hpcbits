#pragma once

#include <vector>
#include <deque>
#include <list>
#include <algorithm>

#include "xxhash.h"
#include <immintrin.h>

template <typename K, typename V>
class MyHashMapWTF2 {
public:

    static const size_t MAGIC_COUNT = 64;
    static const size_t MAGIC_MAX = 256;

    struct MyBucket2
    {
        std::vector<unsigned char> slots; // any elements in slots past digest size are empty slot indicies
        std::vector<unsigned char> digest; // folded hashes
        //std::vector<std::pair<const K, V> > elements; // make this deque to avoid reference invalidation on inserts
        std::deque<std::pair<K, V> > elements; // make this deque to avoid reference invalidation on inserts
    };

    std::vector<MyBucket2> storage;
    size_t item_count;
    size_t bucket_count;

    size_t hash(const K& x) 
    {
        //return std::hash<int>()(x);
        XXH64_hash_t hash = XXH64(&x, sizeof(x), 0); return hash;
    }

    unsigned char fold_hash(size_t x) 
    {
        const unsigned char * pchar = (const unsigned char *) &x;
        const auto res = pchar[0] ^ pchar[1] ^ pchar[2] ^ pchar[3] ^ pchar[4] ^ pchar[5] ^ pchar[6] ^ pchar[7];
        return res;
    }

    MyHashMapWTF2(size_t in_item_count) : item_count(in_item_count)
    {
        bucket_count = item_count/MAGIC_COUNT;
        storage.resize(bucket_count);
		for (auto & bucket : storage)
		{
			bucket.digest.reserve(MAGIC_COUNT);
			bucket.slots.reserve(MAGIC_COUNT);
		}
    }   

    bool set(const K & key, V value) 
    {
        const size_t key_hash = hash(key);
        const unsigned char key_digest = fold_hash(key_hash);
        const size_t bucket_idx = key_hash % bucket_count;
        MyBucket& bucket = storage[bucket_idx];
        const size_t elements_in_bucket = bucket.digest.size();
        for (int i = 0; i < elements_in_bucket; ++i) 
        {
            if (bucket.digest[i] == key_digest) 
	        {
                if (bucket.elements[bucket.slots[i]].first == key) 
                {
                    bucket.elements[bucket.slots[i]].second = value;
                    return true;
                }
    	    }
        }
        return false;
    }

    bool get(const K & key, V& value) {
        const size_t key_hash = hash(key);
        const unsigned char key_digest = fold_hash(key_hash);
        const size_t bucket_idx = key_hash % bucket_count;
        MyBucket& bucket = storage[bucket_idx];
        const size_t elements_in_bucket = bucket.digest.size();
        for (int i = 0; i < elements_in_bucket; ++i) 
        {
            if (bucket.digest[i] == key_digest) 
	        {
                if (bucket.elements[bucket.slots[i]].first == key) 
                {
                    value = bucket.elements[bucket.slots[i]].second;
                    return true;
                }
    	    }
        }
        return false;
    }

    void insert(const K & key, const V & value) // insert doesn't check for existing keys
    {
        const size_t key_hash = hash(key);
        const unsigned char key_digest = fold_hash(key_hash);
        const size_t bucket_idx = key_hash % bucket_count;
        MyBucket& bucket = storage[bucket_idx];
        if (bucket.digest.size() == bucket.slots.size()) 
        {
            if (bucket.digest.size() == 256)
                abort(); // rehash
            bucket.slots.emplace_back(bucket.slots.size());
            bucket.elements.emplace_back(key, value);
            bucket.digest.emplace_back(key_digest);
        }
        else 
        {
            bucket.digest.emplace_back(key_digest);
            bucket.elements[bucket.slots[bucket.digest.size()-1]] = std::make_pair(key, value);
        }
    }

    void insert_or_set(const K & key, const V & value)
    {
        if (!set(key, value))
            insert(key, value);
    }

    bool remove(const K & key) {
        const size_t key_hash = hash(key);
        const unsigned char key_digest = fold_hash(key_hash);
        const size_t bucket_idx = key_hash % bucket_count;
        MyBucket& bucket = storage[bucket_idx];
        const size_t elements_in_bucket = bucket.digest.size();
        for (int i = 0; i < elements_in_bucket; ++i) 
        {
            if (bucket.digest[i] == key_digest) 
	        {
                if (bucket.elements[bucket.slots[i]].first == key) 
                {
                    if ((elements_in_bucket > 1) && (i != elements_in_bucket-1)) 
                    {
                        std::swap(bucket.digest[i], bucket.digest[elements_in_bucket-1]);
                        std::swap(bucket.slots[i], bucket.slots[elements_in_bucket-1]);
                    }
                    bucket.digest.resize(elements_in_bucket-1);
                    return true;
                }
    	    }
        }
        return false;
    }
};
