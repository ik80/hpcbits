#pragma once

#include <cstring>

    static constexpr unsigned long long BTH_powersOfTen[20] = 
    {
        1ULL,
        10ULL,
        100ULL,
        1000ULL,
        10000ULL,
        100000ULL,
        1000000ULL,
        10000000ULL,
        100000000ULL,
        1000000000ULL,
        10000000000ULL,
        100000000000ULL,
        1000000000000ULL,
        10000000000000ULL,
        100000000000000ULL,
        1000000000000000ULL,
        10000000000000000ULL,
        100000000000000000ULL,
        1000000000000000000ULL,
        10000000000000000000ULL
    };

    inline uint64_t bth_atoull(const char* beg, const char* end)
    {
        uint64_t value = 0;
        size_t len = end - beg; // assuming end is past beg;
        switch (len)
            // handle up to 20 digits, assume we're 64-bit
        {
        case 20:
            value += (beg[len - 20] - '0') * 10000000000000000000ULL;
        case 19:
            value += (beg[len - 19] - '0') * 1000000000000000000ULL;
        case 18:
            value += (beg[len - 18] - '0') * 100000000000000000ULL;
        case 17:
            value += (beg[len - 17] - '0') * 10000000000000000ULL;
        case 16:
            value += (beg[len - 16] - '0') * 1000000000000000ULL;
        case 15:
            value += (beg[len - 15] - '0') * 100000000000000ULL;
        case 14:
            value += (beg[len - 14] - '0') * 10000000000000ULL;
        case 13:
            value += (beg[len - 13] - '0') * 1000000000000ULL;
        case 12:
            value += (beg[len - 12] - '0') * 100000000000ULL;
        case 11:
            value += (beg[len - 11] - '0') * 10000000000ULL;
        case 10:
            value += (beg[len - 10] - '0') * 1000000000ULL;
        case 9:
            value += (beg[len - 9] - '0') * 100000000ULL;
        case 8:
            value += (beg[len - 8] - '0') * 10000000ULL;
        case 7:
            value += (beg[len - 7] - '0') * 1000000ULL;
        case 6:
            value += (beg[len - 6] - '0') * 100000ULL;
        case 5:
            value += (beg[len - 5] - '0') * 10000ULL;
        case 4:
            value += (beg[len - 4] - '0') * 1000ULL;
        case 3:
            value += (beg[len - 3] - '0') * 100ULL;
        case 2:
            value += (beg[len - 2] - '0') * 10ULL;
        case 1:
            value += (beg[len - 1] - '0');
        }
        return value;
    }

    // if the dot is present, dot should point to it, otherwise dot should be set to nullptr
    inline double bth_atod(const char* beg, const char* dot, const char* end)
    {
        bool negative = (*beg == '-');
        if (negative) 
            ++beg;
        double res=0.0;
        if (dot + 1 != end && dot != nullptr)
            res = (double)bth_atoull(beg, dot) + (end - dot > 1 ? ((double)bth_atoull(dot+1, end))/ BTH_powersOfTen[end - dot - 1] : 0.0);
        else if (dot == nullptr)
            res = (double)bth_atoull(beg, end);
        else if (dot + 1 == end)
            res = (double)bth_atoull(beg, dot);
        res = negative ? -1.0*res : res;
        return res;
    }
    inline double bth_atod(const char* beg, const char* end)
    {
        return bth_atod(beg, (const char*)std::memchr(beg,'.', end-beg), end);
    }