#pragma once

#include <cstring>

template<typename T>
struct TrieNode
{
    TrieNode * children;
    unsigned long long positions;
    T value;

    TrieNode() : children(nullptr), positions(0) 
    { 
        value = T(); 
    }

    ~TrieNode() 
    {
        struct DeleterTag
        {
            TrieNode* node;
            unsigned char total;
            char cur;
        };
        std::list<DeleterTag> stack;
        stack.push_back({ this, googlerank((const unsigned char*)&positions, 8 * sizeof(unsigned int)),0 });
        while (!stack.empty())
        {
            DeleterTag curTag = stack.front();
            stack.pop_front();
            while (curTag.cur != curTag.total)
            {
                stack.push_back({ &(curTag.node->children[curTag.cur]), (unsigned char) googlerank((const unsigned char*)&(curTag.node->children[curTag.cur].positions), 8 * sizeof(unsigned int)),0 });
                ++curTag.cur;
            }
            std::free(curTag.node->children);
        }
    }
    
    // TODO: Serialization

    inline TrieNode * GetChildNode(unsigned char inPos) const noexcept
    {
        return &(children[googlerank((const unsigned char*)&positions, inPos + 1) - 1]);
    }

    inline TrieNode * AddChildNode(unsigned char inPos) noexcept
    {
        if (children == 0)
        {
            children = (TrieNode<T>*)std::malloc(sizeof(TrieNode<T>));
            new (&children[0]) TrieNode<T>();
            SetPos(inPos);
            return children;
        }
        else
        {
            const unsigned long long count = googlerank((const unsigned char*)&positions, 8*sizeof (unsigned int));
            const unsigned long long rank = googlerank((const unsigned char*)&positions, inPos + 1);
            children = (TrieNode<T>*)std::realloc(children, (count + 1) * sizeof(TrieNode<T>));
            if (rank < count)
                std::memmove(children + rank + 1, children + rank, (count - rank) * sizeof(TrieNode<T>));
            new (&children[rank]) TrieNode<T>();
            SetPos(inPos);
            return &(children[rank]);
        }
    }
    
    inline void RemoveChildNode(unsigned char inPos)
    {
        const unsigned long long count = googlerank((const unsigned char*)&positions, 8*sizeof(positions));
        if (count == 1)
        {
            std::free(children);
            positions = 0;
        }
        else
        {
            const unsigned long long rank = googlerank((const unsigned char*)&positions, inPos + 1);
            if (rank < count)
                std::memmove(children + rank - 1, children + rank, (count - rank) * sizeof(TrieNode<T>));
            children = (TrieNode<T>*)std::realloc(children, (count - 1) * sizeof(TrieNode<T>));
            ClearPos(inPos);
        }
    }

    unsigned long long googlerank(const unsigned char *bm, unsigned long long pos) const noexcept
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
        unsigned long long retval = 0;
        for (; pos > 8ULL; pos -= 8ULL)
            retval += bits_in_char[*bm++];
        return retval + bits_in_char[*bm & ((1ULL << pos) - 1ULL)];
    }

    inline void SetPos(unsigned char inPos)
    {
        positions |= (1ULL << inPos);
    }
    inline void ClearPos(unsigned char inPos)
    {
        positions &= ~(1ULL << inPos);
    }
    inline bool TestPos(unsigned char inPos) const 
    {
        return positions & (1ULL << inPos);
    }
}; // /d1reportSingleClassLayoutTrieNode, g++ also has something
