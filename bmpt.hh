/* Copyright 2023 Kaliuzhnyi Ilia

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*/

#pragma once

#include <vector>
#include <unordered_map>
#include <memory>
#include <cstring>

template<typename T>
class BitmapTree
{
public:

    static const size_t BMP_TREE_HEIGHT = 8;
    static const size_t CHUNK_SIZE = 512;
    static const size_t QWORD_BITS = 64;
    static const size_t CHUNK_SIZE_QWORDS = CHUNK_SIZE/QWORD_BITS;

    // TODO: check unrolling and performance vs bit twiddling hacks
    static size_t googlerank(const unsigned char *bm, size_t pos) noexcept
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
        size_t retval = 0;
        for (; pos > 8ULL; pos -= 8ULL)
            retval += bits_in_char[*bm++];
        return retval + bits_in_char[*bm & ((1ULL << pos) - 1ULL)];
    }
    static inline void set_bit(size_t & bitmap, size_t pos)
    {
        bitmap |= (1ULL << pos);
    }
    static inline void clear_bit(size_t & bitmap, size_t pos)
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

    BitmapTree() {};
    BitmapTree(size_t capacity) : capacity_(capacity) 
    {
        if (capacity_ % CHUNK_SIZE)
            abort();
        std::memset(tree_map, 0x0, BMP_TREE_HEIGHT * sizeof(size_t *));
        size_t counter = capacity_;
        size_t prevCounter;
        int levels = 0;
        do
        {
        	// init element bitmaps to zeroes
            prevCounter = counter;
            counter /= CHUNK_SIZE;
            tree_map[levels] = new size_t[(counter ? counter : 1) * CHUNK_SIZE_QWORDS ]; // bit per element
            std::memset(tree_map[levels], 0x00, (counter ? counter : 1) * CHUNK_SIZE_QWORDS * sizeof(size_t));
            if (!counter)
                top_level_elements_ = prevCounter;
            ++levels;
        }
        while (counter);

        tree_levels_ = levels;
        // rearrange bitmap tree so root is at the top, easier to think about it this way
        for (size_t i = 0; i < tree_levels_ / 2; ++i)
        {
            size_t * tmp = tree_map[i];
            tree_map[i] = tree_map[tree_levels_ - i - 1];
            tree_map[tree_levels_ - i - 1] = tmp;
        }
    }
    ~BitmapTree() 
    {
        for (size_t i = 0; i < tree_levels_; ++i)
            delete[] tree_map[i]; //TODO: leak checks!
    }
    BitmapTree& operator=(const BitmapTree& other) = default; // TODO: tree_map deep copy
    BitmapTree& operator=(BitmapTree&& other) = default; // TODO: tree_map deep copy
    BitmapTree(const BitmapTree& other) = default; // TODO: tree_map deep copy
    BitmapTree(BitmapTree&& other) = default; // TODO: tree_map deep copy
    void swap(BitmapTree& other);

    // clear bits tree_map upwards from lowest level as needed
    void clear_bits(size_t idx) 
    {
        size_t cur_level = tree_levels_ - 1;
        // (idx - idx%CHUNK_SIZE)/QWORD_BITS is the index of the first size_t of the chunk
        clear_bit(tree_map[cur_level][idx/QWORD_BITS],idx%QWORD_BITS);
        bool need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx%CHUNK_SIZE)/QWORD_BITS],0,CHUNK_SIZE/__CHAR_BIT__);
        // setting the bit 
        while (need_to_go_up && cur_level) 
        {
            --cur_level;
            idx /= CHUNK_SIZE;
            clear_bit(tree_map[cur_level][idx/QWORD_BITS],idx%QWORD_BITS);
            need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx%CHUNK_SIZE)/QWORD_BITS],0,CHUNK_SIZE/__CHAR_BIT__);
        }
    }
    // set bits tree_map upwards from lowest level as needed
    void set_bits(size_t idx) 
    {
        size_t cur_level = tree_levels_ - 1;
        // (idx - idx%CHUNK_SIZE)/QWORD_BITS is the index of the first size_t of the chunk
        bool need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx%CHUNK_SIZE)/QWORD_BITS],0,CHUNK_SIZE/__CHAR_BIT__);
        // setting the bit 
        set_bit(tree_map[cur_level][idx/QWORD_BITS],idx%QWORD_BITS);
        while (need_to_go_up && cur_level) 
        {
            --cur_level;
            idx /= CHUNK_SIZE;
            need_to_go_up = memcheck(&tree_map[cur_level][(idx - idx%CHUNK_SIZE)/QWORD_BITS],0,CHUNK_SIZE/__CHAR_BIT__);
            set_bit(tree_map[cur_level][idx/QWORD_BITS],idx%QWORD_BITS);
        }
    }
    bool insert(size_t idx, const T& value) 
    {
        // test
        if (!test(idx)) 
        {
            // insert value into storage
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local std::vector<T> & chunk = storage[0];
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx) 
            {
                prev_bucket_idx = bucket_idx;
                chunk = storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char*>(&tree_map[tree_levels_-1][(idx - idx%CHUNK_SIZE)/QWORD_BITS]),idx%CHUNK_SIZE);
            chunk.insert(chunk.begin() + rank, value);
            // set bits tree_map upwards from lowest level
            set_bits(idx);
            return true;
        }
        return false;
    }
    bool emplace(size_t idx, T&& value)
    {
        // test
        if (!test(idx)) 
        {
            // emplace value into storage
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local std::vector<T> & chunk = storage[0];
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx) 
            {
                prev_bucket_idx = bucket_idx;
                chunk = storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char*>(&tree_map[tree_levels_-1][(idx - idx%CHUNK_SIZE)/QWORD_BITS]),idx%CHUNK_SIZE);
            chunk.emplace(chunk.begin() + rank, value);
            // set bits tree_map upwards from lowest level
            set_bits(idx);
            return true;
        }
        return false;
    }
    bool set(size_t idx, const T& value) 
    {
        // test
        // set value into storage
        if (test(idx)) 
        {
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local std::vector<T> & chunk = storage[0];
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx) 
            {
                prev_bucket_idx = bucket_idx;
                chunk = storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char*>(&tree_map[tree_levels_-1][(idx - idx%CHUNK_SIZE)/QWORD_BITS]),idx%CHUNK_SIZE);
            *(chunk[rank]) = value;
            return true;
        }
        return false;
    }
    bool emplace_into(size_t idx, T&& value)
    {
        // test
        // emplace value into storage
        if (test(idx)) 
        {
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local std::vector<T> & chunk = storage[0];
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx) 
            {
                prev_bucket_idx = bucket_idx;
                chunk = storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char*>(&tree_map[tree_levels_-1][(idx - idx%CHUNK_SIZE)/QWORD_BITS]),idx%CHUNK_SIZE);
            *(chunk[rank]) = value;
            return true;
        }
        return false;
    }
    bool erase(size_t idx) 
    {
        // test
        // erase value from storage
        // clear bits tree_map upwards from lowest level as needed
    }
    bool test(size_t idx) 
    {
        return test_bit(tree_map[tree_levels_-1][idx/QWORD_BITS], idx % QWORD_BITS);
    }
    T&& remove(size_t idx) 
    {
        // test
        // move value from storage
        // clear bits tree_map upwards from lowest level as needed
        // return moved value
    }
    bool get(size_t idx, T& value) 
    {
        // test
        // return reference to value
        if (test(idx)) 
        {
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local std::vector<T> & chunk = storage[0];
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx) 
            {
                prev_bucket_idx = bucket_idx;
                chunk = storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char*>(&tree_map[tree_levels_-1][(idx - idx%CHUNK_SIZE)/QWORD_BITS]),idx%CHUNK_SIZE);
            value = chunk[rank];
            return true;
        }
        return false;
    }
    size_t next(size_t idx) 
    {
        // up and then right/down to next set bit in tree_map
    }
    size_t prev(size_t idx) 
    {
        // up and then left/down to next set bit in tree_map
    }
    T& get_next(size_t& idx) 
    {
        // get(next(idx));
    }
    T& get_prev(size_t& idx)
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
    size_t * tree_map[BMP_TREE_HEIGHT] = {0};
    // idx/CHUNK_SIZE -> chunked element storage
    std::unordered_map<size_t, std::vector<T>> storage; 
    size_t tree_levels_ = 0;
    size_t capacity_ = 0;
    size_t top_level_elements_ = 0;
    size_t size_ = 0;
};

template <typename T>
void swap(BitmapTree<T>& lhs, BitmapTree<T>& rhs) 
{
    lhs.swap(rhs);
}
