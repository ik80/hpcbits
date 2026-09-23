#pragma once

#include <cstddef>
#include <cstring>
#include <utility>
#include <algorithm>

// Sparse is a worst name possible of course. Its just a triangular adjacency matrix for bidirectional graph with bit per edge.
struct PackedAdjMatrix
{
    void setAdjascent(size_t x, size_t y, bool adjascent) noexcept
    {
        if (x == y)
            return;

        if (x > y)
            std::swap(x, y);

		// arithmetic progression sum + offset
        const size_t pos = (((y * (y - 1)) >> 1) + x);
        const size_t ullIdx = pos / (CHAR_BIT * sizeof(size_t));
        const size_t ullOffset = pos % (CHAR_BIT * sizeof(size_t));
        size_t& ull = arrPackedAdjMatrix[ullIdx];
        if (adjascent)
            ull |= (1ULL << ullOffset);
        else
            ull &= ~(1ULL << ullOffset);
    }

    bool isAdjascent(size_t x, size_t y) const noexcept
    {
        if (x == y)
            return true;

        if (x > y)
            std::swap(x, y);

		// arithmetic progression sum + offset
        const size_t pos = (((y * (y - 1)) >> 1) + x);
        const size_t ullIdx = pos / (CHAR_BIT * sizeof(size_t));
        const size_t ullOffset = pos % (CHAR_BIT * sizeof(size_t));
        return (arrPackedAdjMatrix[ullIdx] & (1ULL << ullOffset)) != 0;
    }

    explicit PackedAdjMatrix(size_t size) : numElements(size)
    {
        if (numElements)
        {
            actualSize = (((numElements * (numElements + 1)) / 2) / (CHAR_BIT * sizeof(size_t))) + 1; // Sn = n(n+1)/2 bits
            arrPackedAdjMatrix = new size_t[actualSize];
            std::memset(arrPackedAdjMatrix, 0, actualSize * sizeof(size_t));
        }
    }

    ~PackedAdjMatrix()
    {
        delete[] arrPackedAdjMatrix;
    }

    PackedAdjMatrix(const PackedAdjMatrix& other) // copy constructor
        : PackedAdjMatrix(other.numElements)
    {
        std::memcpy(arrPackedAdjMatrix, other.arrPackedAdjMatrix, actualSize * sizeof(size_t));
    }

    PackedAdjMatrix(PackedAdjMatrix&& other) noexcept // move constructor
        : actualSize(std::exchange(other.actualSize, 0))
        , numElements(std::exchange(other.numElements, 0))
        , arrPackedAdjMatrix(std::exchange(other.arrPackedAdjMatrix, nullptr))
    {
    }

    PackedAdjMatrix& operator=(const PackedAdjMatrix& other) // copy assignment
    {
        if (this == &other)
            return *this;

        PackedAdjMatrix tmp(other);
        *this = std::move(tmp);
        return *this;
    }

    PackedAdjMatrix& operator=(PackedAdjMatrix&& other) noexcept // move assignment
    {
        if (this == &other)
            return *this;

        std::swap(arrPackedAdjMatrix, other.arrPackedAdjMatrix);
        std::swap(actualSize, other.actualSize);
        std::swap(numElements, other.numElements);
        return *this;
    }

    size_t actualSize = 0;
    size_t numElements = 0;
    size_t* arrPackedAdjMatrix = nullptr;
};
