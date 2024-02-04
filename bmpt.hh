/* Copyright 2023 Kaliuzhnyi Ilia

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*/

#pragma once

#include <vector>
#include <unordered_map>
#include <cstdlib>
#include <cstring>

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

    // TODO: check unrolling and performance vs bit twiddling hacks
    static size_t googlerank(const unsigned char *bm, size_t pos) noexcept
    {
        size_t retval = 0;
        for (; pos > 8ULL; pos -= 8ULL)
            retval += bits_in_char[*bm++];
        return retval + bits_in_char[*bm & ((1ULL << pos) - 1ULL)];
    }
    // Compute the rank (bits set) in v from the MSB to pos, bit twiddling hacks edition
    static inline size_t rank_msb(size_t &v, unsigned int pos = 0/* Bit position to count bits upto.*/)
    {
        // Resulting rank of bit at pos goes here.
        size_t r;
        // Shift out bits after given position.
        r = v >> (sizeof(v) * __CHAR_BIT__ - pos); // Count set bits in parallel.
        // r = (r & 0x5555...) + ((r >> 1) & 0x5555...);
        r = r - ((r >> 1) & ~0UL / 3);
        // r = (r & 0x3333...) + ((r >> 2) & 0x3333...);
        r = (r & ~0UL / 5) + ((r >> 2) & ~0UL / 5);
        // r = (r & 0x0f0f...) + ((r >> 4) & 0x0f0f...);
        r = (r + (r >> 4)) & ~0UL / 17;
        // r = r % 255;
        r = (r * (~0UL / 255)) >> ((sizeof(v) - 1) * __CHAR_BIT__);
        return r;
    }
    // select the bit position (from the most-significant bit) with the given count (rank)
    // Output: Resulting position of bit with rank r [1-64], 64 if value has less bits than given rank
    static inline unsigned int select_msb(size_t &v, unsigned int r)
    {
        unsigned int s;
        size_t a, b, c, d; // Intermediate temporaries for bit count.
        unsigned int t;    // Bit count temporary.
        a = v - ((v >> 1) & ~0UL / 3);
        b = (a & ~0UL / 5) + ((a >> 2) & ~0UL / 5);
        c = (b + (b >> 4)) & ~0UL / 0x11;
        d = (c + (c >> 8)) & ~0UL / 0x101;
        t = (d >> 32) + (d >> 48);
        s = 64;
        s -= ((t - r) & 256) >> 3;
        r -= (t & ((t - r) >> 8));
        t = (d >> (s - 16)) & 0xff;
        s -= ((t - r) & 256) >> 4;
        r -= (t & ((t - r) >> 8));
        t = (c >> (s - 8)) & 0xf;
        s -= ((t - r) & 256) >> 5;
        r -= (t & ((t - r) >> 8));
        t = (b >> (s - 4)) & 0x7;
        s -= ((t - r) & 256) >> 6;
        r -= (t & ((t - r) >> 8));
        t = (a >> (s - 2)) & 0x3;
        s -= ((t - r) & 256) >> 7;
        r -= (t & ((t - r) >> 8));
        t = (v >> (s - 1)) & 0x1;
        s -= ((t - r) & 256) >> 8;
        s = 65 - s;
        return s;
    }
    // convert MSB->pos versions of rank and select from BitTwiddlingHacks into LSB
    static inline unsigned int select(size_t &v, unsigned int r)
    {
        size_t rank = rank_msb(v,0);
        if (r > rank)
            return 64;
        return 64 - select_msb(v, rank - r + 1);
    }
    // selects in chunk (cache line) vs a single qword
    static inline unsigned int chunk_select(const unsigned char *bm /*first byte of the chunk*/, unsigned int r/*desired rank*/) 
    {
        size_t rank = 0;
        unsigned int offset = 0;
        const unsigned char * start = bm;
        const unsigned char * qword = bm;
        unsigned int qword_rank = 0;
        while (true) 
        {
            if (bm - start == QWORD_BITS)
                return CHUNK_SIZE; // not found
            if (bm - qword == 8) // save qword boundary and rank
            {
                qword = bm; 
                qword_rank = rank;
                offset += QWORD_BITS;
            }
            rank += bits_in_char[*bm++];
            if (rank >= r)
                break;
        }
        return offset + select(reinterpret_cast<size_t&>(*((size_t*)qword)),r - qword_rank);
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
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local static std::vector<T> * chunk = nullptr;
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx)
            {
                prev_bucket_idx = bucket_idx;
                chunk = &storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
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
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local static std::vector<T> * chunk = nullptr;
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx)
            {
                prev_bucket_idx = bucket_idx;
                chunk = &storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
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
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local static std::vector<T> * chunk = nullptr;
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx)
            {
                prev_bucket_idx = bucket_idx;
                chunk = &(storage[bucket_idx]);
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
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
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local static std::vector<T> * chunk = nullptr;
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx)
            {
                prev_bucket_idx = bucket_idx;
                chunk = &(storage[bucket_idx]);
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
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
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local static std::vector<T> * chunk = nullptr;
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx)
            {
                prev_bucket_idx = bucket_idx;
                chunk = &storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
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
    // TODO: not found??
    bool &&remove(size_t idx)
    {
        // test
        if (test(idx))
        {
            // erase value from storage
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local static std::vector<T> * chunk = nullptr;
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx)
            {
                prev_bucket_idx = bucket_idx;
                chunk = &storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
            // move value from storage
            //T &&res = std::move(chunk[rank]);
            chunk->erase(chunk->begin() + rank);
            // clear bits tree_map upwards from lowest level as needed
            clear_bits(idx);
            return true;
        }
        return false;
    }
    bool get(size_t idx, T &value)
    {
        // test
        // return reference to value
        if (test(idx))
        {
            thread_local static size_t prev_bucket_idx = (size_t)-1;
            thread_local static std::vector<T> * chunk = nullptr;
            const size_t bucket_idx = idx / CHUNK_SIZE;
            if (bucket_idx != prev_bucket_idx)
            {
                prev_bucket_idx = bucket_idx;
                chunk = &storage[bucket_idx];
            }
            const size_t rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[tree_levels_ - 1][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), idx % CHUNK_SIZE);
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
        size_t offset_step = 1;
        size_t offset = 0;
        size_t cur_level = tree_levels_;
        size_t rank = CHUNK_SIZE;
        size_t chunk_rank = CHUNK_SIZE;
        offset = idx;
        //while rank(idx) == rank(chunk), means no set bits after current one at idx, go one level up
        bool need_to_go_up = false;
        do
        {
            --cur_level;
            chunk_rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), CHUNK_SIZE);
            rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), (idx+1) % CHUNK_SIZE);
            need_to_go_up = rank == chunk_rank;
            if(!need_to_go_up || !cur_level)
                break;
            offset += offset_step*(CHUNK_SIZE - idx%CHUNK_SIZE);
            idx /= CHUNK_SIZE;
            offset_step *= CHUNK_SIZE;
        }
        while(true);
        if (!cur_level && need_to_go_up)
            return -1; // nothing ahead
        //select(rank + 1)
        size_t next_idx = chunk_select(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), /*existing_element ? rank + 1 : rank*/rank + 1); // TODO: shotgun programming
        offset += offset_step*(next_idx - idx%CHUNK_SIZE);
        idx = next_idx;
        //while not at bottom level, select(1) and go down to that chunk
        while(cur_level != tree_levels_ - 1)
        {
            ++cur_level;
            idx *= CHUNK_SIZE;
            offset_step /= CHUNK_SIZE;
            rank = chunk_select(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), 1);
            offset = idx += rank; // TODO: shotgun programming
        }
        return offset;
    }
    size_t prev(size_t idx)
    {
        //std::abort();
        // up and then left/down to next set bit in tree_map
        //while rank(idx_in_chunk) == 0, means its the first bit set, go one level up
        //select(cur_level_rank - 1), go down to that chunk
        //while not at bottom level, select(rank(chunk)) and go down to that chunk
        //work magic during this whole process to get an actual offset
        //return magic(select(rank(chunk))) from the bottom level

        // underflow check
        if (!idx)
            return -1;
        bool existing_element = test(idx);
        //premature optimization is the root of all evil
        if (existing_element && test(idx-1))
            return idx-1;
        // up and then right/down to next set bit in tree_map
        size_t offset_step = 1;
        size_t offset = 0;
        size_t cur_level = tree_levels_;
        size_t rank = CHUNK_SIZE;
        size_t chunk_rank = CHUNK_SIZE;
        offset = idx;
        //while rank(idx) == rank(chunk), means no set bits after current one at idx, go one level up
        bool need_to_go_up = false;
        do
        {
            --cur_level;
            rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), (idx + 1) % CHUNK_SIZE);
            need_to_go_up = rank <= 1; // 0 if we are doing prev from non existing values
            if(!need_to_go_up || !cur_level)
                break;
            offset -= offset_step*(idx%CHUNK_SIZE);
            idx /= CHUNK_SIZE;
            offset_step *= CHUNK_SIZE;
        }
        while(true);
        if (!cur_level && need_to_go_up)
            return -1; // nothing behind
        //select(rank - 1)
        size_t next_idx = chunk_select(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), existing_element ? (rank ? rank - 1 : 1) : (rank ? rank : 1));
        offset -= offset_step*(idx%CHUNK_SIZE - next_idx);
        idx = next_idx;
        //while not at bottom level, select(1) and go down to that chunk
        while(cur_level != tree_levels_ - 1)
        {
            ++cur_level;
            idx *= CHUNK_SIZE;
            offset_step /= CHUNK_SIZE;
            chunk_rank = googlerank(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), CHUNK_SIZE);
            rank = chunk_select(reinterpret_cast<const unsigned char *>(&tree_map[cur_level][(idx - idx % CHUNK_SIZE) / QWORD_BITS]), chunk_rank);
            offset = idx += rank; // TODO: shotgun programming
        }
        return offset;
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
