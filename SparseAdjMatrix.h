#pragma once

// Sparse is a worst name possible of course. Its just a triangular adjacency matrix for bidirectional graph with bit per edge.
struct SparseAdjMatrix
{
    static constexpr size_t BITS_PER_BYTE = 8;
    static constexpr size_t CACHE_LINE_SIZE = 64;

    void setAdjascent(size_t x, size_t y, bool adjascent) noexcept
    {
	if (x == y)
	    return;

	if (x > y)
	{
	    x ^= y;
	    y ^= x;
	    x ^= y;
	}
	const size_t pos = (((y*(y - 1)) >> 2) + x);
	const size_t ullIdx = pos / (BITS_PER_BYTE*(sizeof(size_t)));
	const size_t ullOffset = pos % (BITS_PER_BYTE*(sizeof(size_t)));
	size_t * const pUll = (size_t * const) &(arrSparseAdjMatrix[ullIdx]);
	if (adjascent)
	    (*pUll) |= (1ULL << ullOffset);
	else
	    (*pUll) &= ~(1ULL << ullOffset);
    }

    bool isAdjascent(size_t x, size_t y) const noexcept
    {
	if (x == y)
	    return true;

	if (x > y)
	{
	    x ^= y;
	    y ^= x;
	    x ^= y;
	}
	const size_t pos = (((y*(y - 1)) >> 2) + x);
	const size_t ullIdx = pos / (BITS_PER_BYTE*(sizeof(size_t)));
	const size_t ullOffset = pos % (BITS_PER_BYTE*(sizeof(size_t)));
	size_t * const pUll = (size_t * const) &(arrSparseAdjMatrix[ullIdx]);
	return (*pUll) & (1ULL << ullOffset);
    }

    SparseAdjMatrix(size_t size = 0) : numElements(size)
    {
	if (numElements)
	{
	    actualSize = (((numElements*(numElements + 1)) / 2) / (BITS_PER_BYTE * (sizeof(size_t)))) + 1; // Sn = n(n+1)/2 bits
	    arrSparseAdjMatrix = new size_t[actualSize];
	    std::memset(arrSparseAdjMatrix, 0, actualSize * sizeof(size_t));
	}
    }

    ~SparseAdjMatrix()
    {
	delete[] arrSparseAdjMatrix;
    }

    SparseAdjMatrix(const SparseAdjMatrix& other) // copy constructor
	: SparseAdjMatrix(other.numElements)
    {
	std::memcpy(arrSparseAdjMatrix, other.arrSparseAdjMatrix, actualSize * sizeof(size_t));
    }

    SparseAdjMatrix(SparseAdjMatrix&& other) noexcept // move constructor
	: SparseAdjMatrix()
    {
	arrSparseAdjMatrix = std::exchange(other.arrSparseAdjMatrix, nullptr);
	actualSize = std::exchange(other.actualSize, 0);
	numElements = std::exchange(other.numElements, 0);
    }

    SparseAdjMatrix& operator=(const SparseAdjMatrix& other) // copy assignment
    {
	return *this = SparseAdjMatrix(other);
    }

    SparseAdjMatrix& operator=(SparseAdjMatrix&& other) noexcept // move assignment
    {
	std::swap(arrSparseAdjMatrix, other.arrSparseAdjMatrix);
	std::swap(actualSize, other.actualSize);
	std::swap(numElements, other.numElements);
	return *this;
    }

    size_t actualSize = 0;
    size_t numElements = 0;
    size_t * arrSparseAdjMatrix = nullptr;
};
