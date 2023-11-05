/*
 * trbtree.hh
 *
 *  Created on: Dec 13, 2015
 *      Author: kalujny
 */

// borrows heavily from libavl by Ben Pfaff
#ifndef TRBTREE_HH_
#define TRBTREE_HH_

#include <stddef.h>
#include <stdint.h>
#include <cstdio>
#include <cstdlib>

#include <memory>
#include <utility>

#ifndef NDEBUG
#include <vector>
#include <algorithm>
#include <iostream>
#endif

#ifndef UINTPTR_MAX
#error "Unsupported platform"
#endif

#ifndef NDEBUG
extern bool g_TRBTree_dump_stats;
std::vector<void*> op_tags;
#endif

// TODO: save / load
// TODO: implement IBM/Adriver kind of intrusive spinlocking. Element should be struct { THint hint; TKVPair kvPair; }
//       where hint = struct 
//       {
//           TIndex leftIdx; TIndex rightIdx; // indexes into preallocated lists
//           TColor leftColor; TColor rightColor; // children colors
//           TTrail leftTrail; TTrail rightTrail; // children trail types
//           TMark mark; // flags for locking
//           TTag tag; // anti ABA tags
//       }; // of size 8/16 for CASing.
//       
//       Threaded RB Tree algorithm should be linearizable with spinlocking elements in up to 3 recolors/rotations
//       incrementing tags for all elements changed in them, and validating tags up the stack trail to the root
//       Root recoloring can be made as cas, so there's no cache invalidations, or if there is, Load-link/store-conditional

template<typename Key, typename T, class Compare = std::less<Key>, class Allocator = std::allocator<std::pair<const Key, T> > >
class TRBTree
{
// DEBUG
    friend int main(int, char* []); // TODO: debug
    friend void testShit1(); // TODO: debug
    friend void testShit2(); // TODO: debug
// DEBUG
    friend class iterator;
public:

    using key_type = Key;
    using mapped_type = T;
    using reference = std::pair<const Key, T>&;
    using const_reference = const std::pair<const Key, T>&;
    using pointer = std::pair<const Key, T>*;
    using const_pointer = const std::pair<const Key, T>*;
    using value_type = std::pair<const Key, T>;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using key_compare = Compare;
    using allocator_type = Allocator;

private:
    enum TRBTreeNodeColor : unsigned char
    {
        TRB_BLACK = 0, TRB_RED, TRB_COLOR_MAX
    };
    static inline TRBTreeNodeColor otherColor(TRBTreeNodeColor color)
    {
        if (color == TRB_BLACK)
            return TRB_RED;
        else if (color == TRB_RED)
            return TRB_BLACK;
        return color;
    }

    enum TRBTreeNodeTrailTag : unsigned char
    {
        TRB_CHILD = 0, TRB_TRAIL = 1, TRB_TAG_MAX = 2
    };
    static inline TRBTreeNodeTrailTag otherTag(TRBTreeNodeTrailTag tag)
    {
        if (tag == TRB_CHILD)
            return TRB_TRAIL;
        else if (tag == TRB_TRAIL)
            return TRB_CHILD;
        return tag;
    }

    enum TRBTreeNodeChildSide : unsigned char
    {
        TRB_LEFT = 0, TRB_RIGHT = 1, TRB_SIDE_MAX = 2
    };
    static inline TRBTreeNodeChildSide otherSide(TRBTreeNodeChildSide side)
    {
        if (side == TRB_LEFT)
            return TRB_RIGHT;
        else if (side == TRB_RIGHT)
            return TRB_LEFT;
        return side;
    }

    struct TRBTreeNode
    {
        TRBTreeNode() : pLinks{ nullptr, nullptr }, data(value_type()) {}
        TRBTreeNode(const_reference value) : pLinks{ nullptr, nullptr }, data(value) {}
        TRBTreeNode(value_type&& value) : pLinks{ nullptr, nullptr }, data(std::move(value)) {}
        TRBTreeNode* pLinks[TRB_SIDE_MAX];
        value_type data;
    }; // __attribute__((aligned(sizeof(pointer)), packed)); // Alignment to size of pointer allows us to shave at least 2 bits of every pointer for our own needs

public:

    class iterator : public std::iterator< std::input_iterator_tag, 
        TRBTree::value_type,        // value_type
        TRBTree::difference_type,    // difference_type
        TRBTree::pointer,           // pointer
        TRBTree::reference          // reference
    > {
        TRBTree* pTree;
        TRBTreeNode* pTreeNode;
    public:
        explicit iterator(TRBTree* in_pTree, TRBTreeNode* in_pTreeNode) : pTree(in_pTree), pTreeNode(in_pTreeNode) {}
        iterator& operator++() { pTreeNode = pTree->next_node(pTreeNode); return *this; }
        iterator operator++(int) { iterator retval = *this; ++(*this); return retval; }
        iterator& operator--() { pTreeNode = pTree->prev_node(pTreeNode); return *this; }
        iterator operator--(int) { iterator retval = *this; --(*this); return retval; }
        bool operator==(iterator other) const { return pTree == other.pTree && pTreeNode == other.pTreeNode; }
        bool operator!=(iterator other) const { return !(*this == other); }
        reference operator*() const { return pTreeNode->data; }
        pointer operator->() const { return &(pTreeNode->data); }
    };

    class TRBTreeValueCompare
    {
    public:
        using result_type = bool;
        using first_argument_type = value_type;
        using second_argument_type = value_type;

        TRBTreeValueCompare(Compare inComparator) :
            comparator(inComparator) {}
        bool operator() (const value_type& lhs, const value_type& rhs) const
        {
            return comparator(lhs, rhs);
        }
    private:
        Compare comparator;
    };

    using value_compare = TRBTreeValueCompare;

    // Constructors & destructor
    explicit TRBTree(const Compare& comp = Compare(), const Allocator& alloc = Allocator());
    template<class InputIterator> TRBTree(InputIterator first, InputIterator last, const Compare& comp = Compare(), const Allocator& alloc = Allocator());
    TRBTree(const TRBTree& other);
    TRBTree(const TRBTree& other, const Allocator& alloc);
    TRBTree(TRBTree&& other);
    TRBTree(TRBTree&& other, const Allocator& alloc);
    TRBTree(std::initializer_list<value_type> init, const Compare& comp = Compare(), const Allocator& alloc = Allocator());
    TRBTree(std::initializer_list<value_type> init, const Allocator&);
    ~TRBTree();
    TRBTree& operator=(const TRBTree& other);
    TRBTree& operator=(TRBTree&& other);
    TRBTree& operator=(std::initializer_list<value_type> ilist);
    allocator_type get_allocator() const noexcept { return allocator; }

    // Element access
    T& at(const Key& key);
    const T& at(const Key& key) const;
    mapped_type& operator[] (const key_type& k);
    mapped_type& operator[] (key_type&& k);

    // Iterators
    iterator begin() noexcept
    {
        TRBTreeNode* pNode = pRoot;
        while (pNode && get_trail_tag(pNode, TRB_LEFT) == TRB_CHILD)
            pNode = get_link(pNode, TRB_LEFT);

        return iterator(this, pNode);
    }
/*    const_iterator begin() const noexcept;
    const_iterator cbegin() const noexcept;
    reverse_iterator rbegin() noexcept;
    const_reverse_iterator rbegin() const noexcept;
    const_reverse_iterator crbegin() const noexcept;*/
    iterator end() noexcept { return iterator(this, nullptr); } // past the last element points to nullptr
/*    const_iterator end() const noexcept;
    const_iterator cend() const noexcept;
    reverse_iterator rend() noexcept;
    const_reverse_iterator rend() const noexcept;
    const_reverse_iterator crend() const noexcept;*/

    // Capacity
    bool empty() const { return numElements == 0; };
    size_type size() const { return numElements; }
    size_type max_size() const { return (size_t)-1; }

    // Modifiers
    void clear ();
    std::pair<iterator, bool> insert(const_reference value);
    std::pair<iterator, bool> insert(value_type&& value);
//    std::pair<iterator, bool> insert (const_iterator hint, const_reference value);
    template<class InputIterator>
    void insert (InputIterator first, InputIterator last);
    iterator erase (iterator pos);
//    iterator erase (const_iterator pos);
    void erase (iterator first, iterator last);
//    iterator erase (const_iterator first, const_iterator last);
    size_type erase (const key_type& key);
    void swap (TRBTree & other);

    // Lookup
    size_type count (const Key& key) const;
    iterator find (const Key& key);
//    const_iterator find (const Key& key) const;
    std::pair<iterator, iterator> equal_range (const Key& key);
//    std::pair<const_iterator, const_iterator> equal_range (const Key& key) const;
    iterator lower_bound (const Key& key);
//    const_iterator lower_bound (const Key& key) const;
    iterator upper_bound (const Key& key);
//    const_iterator upper_bound (const Key& key) const;
    key_compare key_comp () const { return comparator; };
    value_compare value_comp () const;
private:
    inline TRBTreeNode * try_insert (value_type&& value, bool& inserted);
    inline TRBTreeNode * try_remove (const key_type & key);
    inline TRBTreeNode * try_find (const key_type & key) const noexcept;
    inline TRBTreeNode* next_node(TRBTreeNode* curNode);
    inline TRBTreeNode* prev_node(TRBTreeNode* curNode);

    // Node helper functions
    static inline void set_link (TRBTreeNode * pNode, TRBTreeNode * pLink, TRBTreeNodeChildSide childSide = TRB_SIDE_MAX, TRBTreeNodeTrailTag trailTag =
                                                 TRB_TAG_MAX);
    static inline TRBTreeNode * get_link (const TRBTreeNode * pNode, const TRBTreeNodeChildSide childSide = TRB_SIDE_MAX) noexcept;
    static inline void set_trail_tag (TRBTreeNode * pNode, TRBTreeNodeChildSide childSide = TRB_SIDE_MAX, TRBTreeNodeTrailTag trailTag = TRB_TAG_MAX);
    static inline TRBTreeNodeTrailTag get_trail_tag (const TRBTreeNode * pNode, const TRBTreeNodeChildSide childSide = TRB_SIDE_MAX) noexcept;
    static inline TRBTreeNodeColor get_color (const TRBTreeNode * pNode) noexcept;
    static inline void set_color (TRBTreeNode * pNode, TRBTreeNodeColor color);

    // DEBUG!
    /* Prints the structure of |node|,
       which is |level| levels from the top of the tree. */
    void
    print_tree_structure (struct TRBTreeNode * node, int level)
    {
      int i;

      /* You can set the maximum level as high as you like.
         Most of the time, you'll want to debug code using small trees,
         so that a large |level| indicates a ``loop'', which is a bug. */
      if (level > 16)
        {
          printf ("[...]");
          return;
        }

      if (node == nullptr)
        {
          printf ("<nil>");
          return;
        }

      printf ("%d(", node->data);

      for (i = 0; i <= 1; i++)
        {
          if (get_trail_tag(node, i ? TRB_RIGHT : TRB_LEFT) == TRB_CHILD)
            {
              if (get_link(node, i ? TRB_RIGHT : TRB_LEFT) == node)
                printf ("loop");
              else
                print_tree_structure (get_link(node, i ? TRB_RIGHT : TRB_LEFT), level + 1);
            }
          else if (get_link(node, i ? TRB_RIGHT : TRB_LEFT) != nullptr)
            printf (">%d",
                    get_link(node, i ? TRB_RIGHT : TRB_LEFT)->data.first);
          else
            printf (">>");

          if (i == 0)
            fputs (", ", stdout);
        }

      putchar (')');
    }

    /* Prints the entire structure of |tree| with the given |title|. */
    void
    print_whole_tree (const char *title)
    {
      printf ("%s: ", title);
      print_tree_structure (pRoot, 0);
      putchar ('\n');
    }
    // END DEBUG!

    static constexpr size_t MAX_TREE_HEIGHT = 128;
    TRBTreeNode * pRoot = nullptr;
    const key_compare & comparator;
    const allocator_type & allocator;
    size_t numElements;
};

// Node helper functions
template<typename Key, typename T, class Compare, class Allocator>
inline void TRBTree<Key, T, Compare, Allocator>::set_link (TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * pNode,
                                                                  TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * pLink,
                                                                  TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeChildSide childSide,
                                                                  TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeTrailTag trailTag)
{
    pNode->pLinks[childSide] = (TRBTreeNode *) (((size_t) pLink & ~3ULL) | (trailTag == TRB_TAG_MAX ? (get_trail_tag(pNode, childSide)  == TRB_TRAIL ? 2ULL : 0ULL) : (trailTag == TRB_TRAIL ? 2ULL : 0ULL))
                    | (TRBTree<Key, T, Compare, Allocator>::get_color (pNode) == TRB_RED ? 1ULL : 0ULL));
#ifndef NDEBUG
    if (g_TRBTree_dump_stats)
        op_tags.emplace_back(pNode);
#endif
}

template<typename Key, typename T, class Compare, class Allocator>
inline typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * TRBTree<Key, T, Compare, Allocator>::get_link (
                const TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * pNode, const TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeChildSide childSide) noexcept
{
    return (TRBTreeNode *) (((size_t) pNode->pLinks[childSide]) & ~3ULL);
}

template<typename Key, typename T, class Compare, class Allocator>
inline void TRBTree<Key, T, Compare, Allocator>::set_trail_tag (TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * pNode,
                                                                       TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeChildSide childSide,
                                                                       TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeTrailTag trailTag)
{
    pNode->pLinks[childSide] = (TRBTreeNode *) (((size_t) pNode->pLinks[childSide] & ~2ULL) | (trailTag == TRB_TRAIL ? 2ULL : 0ULL));
#ifndef NDEBUG
    if (g_TRBTree_dump_stats)
        op_tags.emplace_back(pNode);
#endif
}

template<typename Key, typename T, class Compare, class Allocator>
inline typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeTrailTag TRBTree<Key, T, Compare, Allocator>::get_trail_tag (
                const TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * pNode, const TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeChildSide childSide) noexcept
{
    return (typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeTrailTag) (((size_t)pNode->pLinks[childSide] & 2ULL) >> 1);
}

template<typename Key, typename T, class Compare, class Allocator>
inline void TRBTree<Key, T, Compare, Allocator>::set_color (TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * pNode,
                                                                   TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeColor color)
{
    pNode->pLinks[TRB_LEFT] = (TRBTreeNode *) (((size_t) pNode->pLinks[TRB_LEFT] & ~1ULL) | (color == TRB_RED ? 1ULL : 0ULL));
#ifndef NDEBUG
    if (g_TRBTree_dump_stats)
        op_tags.emplace_back(pNode);
#endif
}

template<typename Key, typename T, class Compare, class Allocator>
inline typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeColor TRBTree<Key, T, Compare, Allocator>::get_color (
                const TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * pNode) noexcept
{
    return (typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNodeColor) ((size_t)pNode->pLinks[TRB_LEFT] & 1ULL);
}

template<typename Key, typename T, class Compare, class Allocator>
TRBTree<Key, T, Compare, Allocator>::TRBTree (const Compare& inComp, const Allocator& inAlloc) :
                pRoot (0), comparator (inComp), allocator (inAlloc), numElements (0)
{
}

template<typename Key, typename T, class Compare, class Allocator>
template<class InputIterator>
TRBTree<Key, T, Compare, Allocator>::TRBTree (InputIterator first, InputIterator last, const Compare& inComp, const Allocator& inAlloc) :
                pRoot (0), comparator (inComp), allocator (inAlloc), numElements (0)
{
    // TODO: insert elements from first to last
}


template<typename Key, typename T, class Compare, class Allocator>
TRBTree<Key, T, Compare, Allocator>::TRBTree (const TRBTree<Key, T, Compare, Allocator> & other) :
                pRoot (0), comparator (other.comparator), allocator (other.allocator), numElements (0)
{
    TRBTreeNode *p;
    TRBTreeNode *q;
    TRBTreeNode rp, rq;

    numElements = other.numElements;
    if (!numElements)
        return;

    p = &rp;
    set_link (p, other.pRoot, TRB_LEFT, TRB_CHILD);

    q = &rq;
    set_link (q, 0, TRB_LEFT, TRB_TRAIL);

    while (true)
    {
        if (get_trail_tag (p, TRB_LEFT) == TRB_CHILD)
        {
            p = get_link (p, TRB_LEFT);
            q = get_link (q, TRB_LEFT);
        }
        else
        {
            while (get_trail_tag (p, TRB_RIGHT) == TRB_TRAIL)
            {
                p = get_link (p, TRB_RIGHT);
                if (!p)
                {
                    set_link (q, 0, TRB_RIGHT, TRB_CHILD);
                    pRoot = get_link (q, TRB_LEFT);
                    return;
                }
                q = get_link (q, TRB_RIGHT);
            }

            p = get_link (p, TRB_RIGHT);
            q = get_link (q, TRB_RIGHT);
        }
    }
}

template<typename Key, typename T, class Compare, class Allocator>
TRBTree<Key, T, Compare, Allocator>::TRBTree(TRBTree<Key, T, Compare, Allocator>&& other) :
    pRoot(0), comparator(Compare()), allocator(Allocator()), numElements(0)
{
    abort();
}

template<typename Key, typename T, class Compare, class Allocator>
TRBTree<Key, T, Compare, Allocator>::~TRBTree ()
{
    TRBTreeNode * p;
    TRBTreeNode * n;

    p = pRoot;
    while (p && get_trail_tag (p, TRB_LEFT) == TRB_CHILD)
        p = get_link (p, TRB_LEFT);

    while (p)
    {
        n = get_link (p, TRB_RIGHT);
        if (get_trail_tag (p, TRB_RIGHT) == TRB_CHILD)
            while (get_trail_tag (n, TRB_LEFT) == TRB_CHILD)
                n = get_link (n, TRB_LEFT);

        delete p;
        if (n == (TRBTreeNode *)&pRoot)
            break;
        p = n;
    }
}

template<typename Key, typename T, class Compare, class Allocator>
typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * TRBTree<Key, T, Compare, Allocator>::try_insert (TRBTree<Key, T, Compare, Allocator>::value_type&& item, bool & inserted)
{
#ifndef NDEBUG
    TRBTreeNode * sa[MAX_TREE_HEIGHT] = {0}; // Siblings Nodes on stack.
    TRBTreeNodeTrailTag sa_tag[MAX_TREE_HEIGHT] = {TRB_TAG_MAX}; // Whether simblind is a child or a thread
    int max_k = 1;
    // please note that sibling direction can be attained from otherside(da[idx]), check the idx
    if (g_TRBTree_dump_stats) 
    {
        std::cout << "inserting " << item.first << "->" << item.second << std::endl;
    }
#endif

    TRBTreeNode * pa[MAX_TREE_HEIGHT] = {0}; // Nodes on stack
    TRBTreeNodeChildSide da[MAX_TREE_HEIGHT] = {TRB_SIDE_MAX}; // Directions moved from stack nodes
    int k = 0; // Stack height

    TRBTreeNode * p; // Traverses tree looking for insertion point. 
    TRBTreeNode * n; // Newly inserted node. 
    TRBTreeNodeChildSide dir = TRB_SIDE_MAX; // Side of |p| on which |n| is inserted. 

    da[0] = TRB_LEFT;
    pa[0] = (TRBTreeNode *) &pRoot; // hoho
    k = 1;
    inserted = false;

    p = pRoot;
    if (p) 
    {
        while(true)
        {
            if (comparator(item.first, p->data.first))
                dir = TRB_LEFT;
            else
            {
                if (comparator(p->data.first, item.first))
                    dir = TRB_RIGHT;
                else 
                {
                    return p;
                }
            }
            pa[k] = p;
            da[k++] = dir;
#ifndef NDEBUG
            if (g_TRBTree_dump_stats) 
            {
                sa[k] = get_link(pa[k-1], otherSide(da[k-1]));
                sa_tag[k] = get_trail_tag(pa[k-1], otherSide(da[k-1]));
            }
#endif
            if (get_trail_tag(p, dir) == TRB_TRAIL)
                break;
            p = get_link(p, dir);
        }
    }
    else
    {
        p = (TRBTreeNode *) &pRoot;
        dir = TRB_LEFT;
    }

#ifndef NDEBUG
    max_k = k;
#endif

    n = new TRBTreeNode (std::forward<value_type>(item));
    inserted = true;
    ++numElements;

    set_trail_tag (n, TRB_LEFT, TRB_TRAIL);
    set_trail_tag (n, TRB_RIGHT, TRB_TRAIL);
    set_link (n, get_link (p, dir), dir);
    if (pRoot)
    {
        set_trail_tag (p, dir, TRB_CHILD);
        set_link (n, p, otherSide (dir));
    }
    else
        set_link (n, 0, TRB_RIGHT);

    set_link (p, n, dir);
    set_color (n, TRB_RED);

    while (k >= 3 && get_color (pa[k - 1]) == TRB_RED)
    {
        if (da[k - 2] == TRB_LEFT)
        {
            TRBTreeNode * y = get_link (pa[k - 2], TRB_RIGHT);
            if (get_trail_tag (pa[k - 2], TRB_RIGHT) == TRB_CHILD && get_color (y) == TRB_RED)
            {
                set_color (pa[k - 1], TRB_BLACK);
                set_color (y, TRB_BLACK);
                set_color (pa[k - 2], TRB_RED);
                k -= 2;
            }
            else
            {
                TRBTreeNode * x;

                if (da[k - 1] == TRB_LEFT)
                    y = pa[k - 1];
                else
                {
                    x = pa[k - 1];
                    y = get_link (x, TRB_RIGHT);
                    set_link (x, get_link (y, TRB_LEFT), TRB_RIGHT);
                    set_link (y, x, TRB_LEFT);
                    set_link (pa[k - 2], y, TRB_LEFT);

                    if (get_trail_tag (y, TRB_LEFT) == TRB_TRAIL)
                    {
                        set_trail_tag (y, TRB_LEFT, TRB_CHILD);
                        set_trail_tag (x, TRB_RIGHT, TRB_TRAIL);
                        set_link (x, y, TRB_RIGHT);
                    }
                }

                x = pa[k - 2];
                set_color (x, TRB_RED);
                set_color (y, TRB_BLACK);
                set_link (x, get_link (y, TRB_RIGHT), TRB_LEFT);
                set_link (y, x, TRB_RIGHT);
                set_link (pa[k - 3], y, da[k - 3]);

                if (get_trail_tag (y, TRB_RIGHT) == TRB_TRAIL)
                {
                    set_trail_tag (y, TRB_RIGHT, TRB_CHILD);
                    set_trail_tag (x, TRB_LEFT, TRB_TRAIL);
                    set_link (x, y, TRB_LEFT);
                }
                break;
            }
        }
        else
        {
            TRBTreeNode * y = get_link (pa[k - 2], TRB_LEFT);

            if (get_trail_tag (pa[k - 2], TRB_LEFT) == TRB_CHILD && get_color (y) == TRB_RED)
            {
                set_color (pa[k - 1], TRB_BLACK);
                set_color (y, TRB_BLACK);
                set_color (pa[k - 2], TRB_RED);
                k -= 2;
            }
            else
            {
                TRBTreeNode * x;

                if (da[k - 1] == TRB_RIGHT)
                    y = pa[k - 1];
                else
                {
                    x = pa[k - 1];
                    y = get_link (x, TRB_LEFT);
                    set_link (x, get_link (y, TRB_RIGHT), TRB_LEFT);
                    set_link (y, x, TRB_RIGHT);
                    set_link (pa[k - 2], y, TRB_RIGHT);

                    if (get_trail_tag (y, TRB_RIGHT) == TRB_TRAIL)
                    {
                        set_trail_tag (y, TRB_RIGHT, TRB_CHILD);
                        set_trail_tag (x, TRB_LEFT, TRB_TRAIL);
                        set_link (x, y, TRB_LEFT);
                    }
                }

                x = pa[k - 2];
                set_color (x, TRB_RED);
                set_color (y, TRB_BLACK);
                set_link (x, get_link (y, TRB_LEFT), TRB_RIGHT);
                set_link (y, x, TRB_LEFT);
                set_link (pa[k - 3], y, da[k - 3]);

                if (get_trail_tag (y, TRB_LEFT) == TRB_TRAIL)
                {
                    set_trail_tag (y, TRB_LEFT, TRB_CHILD);
                    set_trail_tag (x, TRB_RIGHT, TRB_TRAIL);
                    set_link (x, y, TRB_RIGHT);

                }
                break;
            }
        }
    }
    if (get_link((TRBTreeNode *)&pRoot, TRB_LEFT) && (get_color(get_link((TRBTreeNode *)&pRoot, TRB_LEFT)) != TRB_BLACK))
        set_color(get_link((TRBTreeNode *)&pRoot, TRB_LEFT), TRB_BLACK);

#ifndef NDEBUG
    if (g_TRBTree_dump_stats) 
    {
        // compute and output the debug stats: keys, height from root
        std::sort(op_tags.begin(), op_tags.end());
        op_tags.erase(std::unique(op_tags.begin(), op_tags.end()), op_tags.end());

        std::sort(op_tags.begin(), op_tags.end(), [](void * & lhs, void * & rhs){ return ((TRBTreeNode*)lhs)->data.first < ((TRBTreeNode*)rhs)->data.first; });
        for (auto & ptr : op_tags) 
        {
            bool found = false;   
            for (size_t i = 0; i <= max_k; ++i) 
            {
                if (pa[i] == ((TRBTreeNode*)ptr)) 
                {
                    std::cout << "Key " << ((TRBTreeNode*)ptr)->data.first << " at height " << i << std::endl;
                    found = true;
                    break;
                }
                else if (sa[i] == ((TRBTreeNode*)ptr)) 
                {
                    std::cout << "Key " << ((TRBTreeNode*)ptr)->data.first << " at height " << i << std::endl;
                    found = true;
                    break;
                }
            }
            if (!found)  
            {
                if (n == ((TRBTreeNode*)ptr)) 
                {
                    std::cout << "New item " << ((TRBTreeNode*)ptr)->data.first << std::endl;
                }
                else 
                    std::cout << "Element with key " << ((TRBTreeNode*)ptr)->data.first << " not found" << std::endl;
            }
        }
        op_tags.clear();
    }
#endif

    return n;
}

template<typename Key, typename T, class Compare, class Allocator>
typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * TRBTree<Key, T, Compare, Allocator>::try_remove (const TRBTree<Key, T, Compare, Allocator>::key_type & key)
{
    // abort(); // this never worked properly
    TRBTreeNode * pa[MAX_TREE_HEIGHT] = {0}; /* Nodes on stack. */
    TRBTreeNodeChildSide da[MAX_TREE_HEIGHT] = {TRB_SIDE_MAX}; /* Directions moved from stack nodes. */
    int k = 0; /* Stack height. */

    TRBTreeNode * p;
    TRBTreeNode * res;
    TRBTreeNodeChildSide dir = TRB_SIDE_MAX;

    if (pRoot == nullptr)
        return nullptr;

    p = (TRBTreeNode *)&pRoot;
    while(true)
    {
        if (key_comp()(key, p->data.first))
            dir = TRB_LEFT;
        else
        {
            if (key_comp()(p->data.first, key))
                dir = TRB_RIGHT;
            else
                break;
        }
        pa[k] = p;
        da[k++] = dir;
        if (get_trail_tag(p, dir) == TRB_TRAIL)
            return nullptr;
        p = get_link(p, dir);
    }
    res = p;

    if (get_trail_tag(p, TRB_RIGHT) == TRB_TRAIL)
    {
        if (get_trail_tag(p, TRB_LEFT) == TRB_CHILD)
        {
            TRBTreeNode *t = get_link(p, TRB_LEFT);
            while (get_trail_tag(t, TRB_RIGHT) == TRB_CHILD)
                t = get_link(t, TRB_RIGHT);
            set_link(t, get_link(p, TRB_RIGHT), TRB_RIGHT);
            set_link(pa[k - 1], get_link(p, TRB_LEFT), da[k - 1]);
        }
        else
        {
            set_link(pa[k - 1], get_link(p, da[k - 1]), da[k - 1]);
            if (pa[k - 1] != (TRBTreeNode *) &pRoot)
                set_trail_tag(pa[k - 1], da[k - 1], TRB_TRAIL);
        }
    }
    else
    {
        TRBTreeNodeColor t;
        TRBTreeNode * r = get_link(p, TRB_RIGHT);

        if (get_trail_tag(r, TRB_LEFT) == TRB_TRAIL)
        {
            set_link(r, get_link(p, TRB_LEFT), TRB_LEFT);
            set_trail_tag(r, TRB_LEFT, get_trail_tag(p, TRB_LEFT));
            if (get_trail_tag(r, TRB_LEFT) == TRB_CHILD)
            {
                TRBTreeNode * t = get_link(r, TRB_LEFT);
                while (get_trail_tag(t, TRB_RIGHT) == TRB_CHILD)
                    t = get_link(t, TRB_RIGHT);
                set_link(t, r, TRB_RIGHT);
            }
            set_link(pa[k - 1], r, da[k - 1]);
            t = get_color(r);
            set_color(r, get_color(p));
            set_color(p, t);
            da[k] = TRB_RIGHT;
            pa[k++] = r;
        }
        else
        {
            TRBTreeNode * s;
            int j = k++;

            for (;;)
            {
                da[k] = TRB_LEFT;
                pa[k++] = r;
                s = get_link(r, TRB_LEFT);
                if (get_trail_tag(s, TRB_LEFT) == TRB_TRAIL)
                    break;
                r = s;
            }

            da[j] = TRB_RIGHT;
            pa[j] = s;
            if (get_trail_tag(s, TRB_RIGHT) == TRB_CHILD)
                set_link(r, get_link(s, TRB_RIGHT), TRB_LEFT);
            else
            {
                set_link(r, s, TRB_LEFT);
                set_trail_tag(r, TRB_LEFT, TRB_TRAIL);
            }

            set_link(s, get_link(p, TRB_LEFT), TRB_LEFT);
            if (get_trail_tag(p, TRB_LEFT) == TRB_CHILD)
            {
                TRBTreeNode * t = get_link(p, TRB_LEFT);
                while (get_trail_tag(t, TRB_RIGHT) == TRB_CHILD)
                    t = get_link(t, TRB_RIGHT);
                set_link(t, s, TRB_RIGHT);
                set_trail_tag(s, TRB_LEFT, TRB_CHILD);
            }

            set_link(s, get_link(p, TRB_RIGHT), TRB_RIGHT);
            set_trail_tag(s, TRB_RIGHT, TRB_CHILD);

            t = get_color(s);
            set_color(s, get_color(p));
            set_color(p, t);

            set_link(pa[j - 1], s, da[j - 1]);
        }
    }

    if (get_color(p) == TRB_BLACK)
    {
        for (; k > 1; k--)
        {
            if (get_trail_tag(pa[k - 1], da[k - 1]) == TRB_CHILD)
            {
                TRBTreeNode * x = get_link(pa[k - 1], da[k - 1]);
                if (get_color(x) == TRB_RED)
                {
                    set_color(x, TRB_BLACK);
                    break;
                }
            }

            if (da[k - 1] == TRB_LEFT)
            {
                TRBTreeNode * w = get_link(pa[k - 1], TRB_RIGHT);

                if (get_color(w) == TRB_RED)
                {
                    set_color(w, TRB_BLACK);
                    set_color(pa[k - 1], TRB_RED);

                    set_link(pa[k - 1], get_link(w, TRB_LEFT), TRB_RIGHT);
                    set_link(w, pa[k - 1], TRB_LEFT);
                    set_link(pa[k - 2], w, da[k - 2]);

                    pa[k] = pa[k - 1];
                    da[k] = TRB_LEFT;
                    pa[k - 1] = w;
                    k++;

                    w = get_link(pa[k - 1], TRB_RIGHT);
                }

                if ((get_trail_tag(w, TRB_LEFT) == TRB_TRAIL || get_color(get_link(w, TRB_LEFT)) == TRB_BLACK) && (get_trail_tag(w, TRB_RIGHT) == TRB_TRAIL || get_color(get_link(w, TRB_RIGHT)) == TRB_BLACK))
                {
                    set_color(w, TRB_RED);
                }
                else
                {
                    if (get_trail_tag(w, TRB_RIGHT) == TRB_TRAIL || get_color(get_link(w, TRB_RIGHT)) == TRB_BLACK)
                    {
                        TRBTreeNode * y = get_link(w, TRB_LEFT);
                        set_color(y, TRB_BLACK);
                        set_color(w, TRB_RED);
                        set_link(w, get_link(y, TRB_RIGHT), TRB_LEFT);
                        set_link(y, w, TRB_RIGHT);
                        set_link(pa[k - 1], y, TRB_RIGHT);
                        w = y;

                        if (get_trail_tag(w, TRB_RIGHT) == TRB_TRAIL)
                        {
                            set_trail_tag(w, TRB_RIGHT, TRB_CHILD);
                            set_trail_tag(get_link(w, TRB_RIGHT), TRB_LEFT, TRB_TRAIL);
                            set_link(get_link(w, TRB_RIGHT), w, TRB_LEFT);
                        }
                    }

                    set_color(w, get_color(pa[k - 1]));
                    set_color(pa[k - 1], TRB_BLACK);
                    set_color(get_link(w, TRB_RIGHT), TRB_BLACK);
                    set_link(pa[k - 1], get_link(w, TRB_LEFT), TRB_RIGHT);
                    set_link(w, pa[k - 1], TRB_LEFT);
                    set_link(pa[k - 2], w, da[k - 2]);

                    if (get_trail_tag(w, TRB_LEFT) == TRB_TRAIL)
                    {
                        set_trail_tag(w, TRB_LEFT, TRB_CHILD);
                        set_trail_tag(pa[k - 1], TRB_RIGHT, TRB_TRAIL);
                        set_link(pa[k - 1], w, TRB_RIGHT);
                    }
                    break;
                }
            }
            else
            {
                TRBTreeNode * w = get_link(pa[k - 1], TRB_LEFT);

                if (get_color(w) == TRB_RED)
                {
                    set_color(w, TRB_BLACK);
                    set_color(pa[k - 1], TRB_RED);

                    set_link(pa[k - 1], get_link(w, TRB_RIGHT), TRB_LEFT);
                    set_link(w, pa[k - 1], TRB_RIGHT);
                    set_link(pa[k - 2], w, da[k - 2]);

                    pa[k] = pa[k - 1];
                    da[k] = TRB_RIGHT;
                    pa[k - 1] = w;
                    k++;


                    w = get_link(pa[k - 1], TRB_LEFT);
                }

                if ((get_trail_tag(w, TRB_LEFT) == TRB_TRAIL || get_color(get_link(w, TRB_LEFT))== TRB_BLACK) && (get_trail_tag(w, TRB_RIGHT) == TRB_TRAIL || get_color(get_link(w, TRB_RIGHT)) == TRB_BLACK))
                {
                    set_color(w, TRB_RED);
                }
                else
                {
                    if (get_trail_tag(w, TRB_LEFT) == TRB_TRAIL || get_color(get_link(w, TRB_LEFT)) == TRB_BLACK)
                    {
                        TRBTreeNode * y = get_link(w, TRB_RIGHT);
                        set_color(y, TRB_BLACK);
                        set_color(w, TRB_RED);
                        set_link(w, get_link(y, TRB_LEFT), TRB_RIGHT);
                        set_link(y, w, TRB_LEFT);
                        set_link(pa[k - 1], y, TRB_LEFT);
                        w = y;

                        if (get_trail_tag(w, TRB_RIGHT) == TRB_TRAIL)
                        {
                            set_trail_tag(w, TRB_LEFT, TRB_CHILD);
                            set_trail_tag(get_link(w, TRB_LEFT), TRB_RIGHT, TRB_TRAIL);
                            set_link(get_link(w, TRB_LEFT), w, TRB_RIGHT);
                        }
                    }

                    set_color(w, get_color(pa[k - 1]));
                    set_color(pa[k - 1], TRB_BLACK);
                    set_color(get_link(w, TRB_LEFT), TRB_BLACK);

                    set_link(pa[k - 1], get_link(w, TRB_RIGHT), TRB_LEFT);
                    set_link(w, pa[k - 1], TRB_RIGHT);
                    set_link(pa[k - 2], w, da[k - 2]);

                    if (get_trail_tag(w, TRB_RIGHT) == TRB_TRAIL)
                    {
                        set_trail_tag(w, TRB_RIGHT, TRB_CHILD);
                        set_trail_tag(pa[k - 1], TRB_LEFT, TRB_TRAIL);
                        set_link(pa[k - 1], w, TRB_LEFT);
                    }
                    break;
                }
            }
        }

        if (get_link((TRBTreeNode *)&pRoot, TRB_LEFT) && (get_color(get_link((TRBTreeNode *)&pRoot, TRB_LEFT)) != TRB_BLACK))
            set_color(get_link((TRBTreeNode *)&pRoot, TRB_LEFT), TRB_BLACK);
    }

    delete res;
    --numElements;

    return (TRBTreeNode *) 1; // TODO: return next(res)
}

template<typename Key, typename T, class Compare, class Allocator>
typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * TRBTree<Key, T, Compare, Allocator>::try_find (const TRBTree<Key, T, Compare, Allocator>::key_type & key) const noexcept
{
    TRBTreeNode * p = pRoot;
    if (p) 
    {
        while(true)
        {
            TRBTreeNodeChildSide dir;
            if (key_comp()(key, p->data.first))
                dir = TRB_LEFT;
            else
            {
                if (key_comp()(p->data.first, key))
                    dir = TRB_RIGHT;
                else
                    break;
            }
            if (get_trail_tag(p, dir) == TRB_TRAIL)
                return nullptr;
            p = get_link(p, dir);
        }
    }
    return p;
}

template<typename Key, typename T, class Compare, class Allocator>
typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode * TRBTree<Key, T, Compare, Allocator>::next_node(typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode* curNode)
{
    if (get_trail_tag(curNode, TRB_RIGHT) == TRB_TRAIL)
    {
        return get_link(curNode, TRB_RIGHT);
    }
    else
    {
        curNode = get_link(curNode, TRB_RIGHT);
        while (get_trail_tag(curNode, TRB_LEFT) == TRB_CHILD)
            curNode = get_link(curNode, TRB_LEFT);
        return curNode;
    }
}

template<typename Key, typename T, class Compare, class Allocator>
typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode* TRBTree<Key, T, Compare, Allocator>::prev_node(typename TRBTree<Key, T, Compare, Allocator>::TRBTreeNode* curNode)
{
    if (get_trail_tag(curNode, TRB_LEFT) == TRB_TRAIL)
    {
        return get_link(curNode, TRB_LEFT);
    }
    else
    {
        curNode = get_link(curNode, TRB_LEFT);
        while (get_trail_tag(curNode, TRB_RIGHT) == TRB_CHILD)
            curNode = get_link(curNode, TRB_RIGHT);
        return curNode;
    }
}

template<typename Key, typename T, class Compare, class Allocator>
std::pair<typename TRBTree<Key, T, Compare, Allocator>::iterator, bool> TRBTree<Key, T, Compare, Allocator>::insert(typename TRBTree<Key, T, Compare, Allocator>::const_reference value) 
{
    bool inserted;
    auto value_copy = value;
    TRBTreeNode* pResultNode = try_insert(std::move(value_copy), inserted);
    return std::make_pair(TRBTree<Key, T, Compare, Allocator>::iterator(this, pResultNode),inserted);
}

template<typename Key, typename T, class Compare, class Allocator>
std::pair<typename TRBTree<Key, T, Compare, Allocator>::iterator, bool> TRBTree<Key, T, Compare, Allocator>::insert(typename TRBTree<Key, T, Compare, Allocator>::value_type && value)
{
    bool inserted;
    TRBTreeNode* pResultNode = try_insert(std::forward<value_type>(value), inserted);
    return std::make_pair(TRBTree<Key, T, Compare, Allocator>::iterator(this, pResultNode), inserted);
}

template<typename Key, typename T, class Compare, class Allocator>
T& TRBTree<Key, T, Compare, Allocator>::operator[] (const Key& k) 
{
    return (*((this->insert(std::make_pair(k, mapped_type()))).first)).second;
}

template<typename Key, typename T, class Compare, class Allocator>
T& TRBTree<Key, T, Compare, Allocator>::operator[] (Key&& k) 
{
    return (*((this->insert(std::make_pair(k, mapped_type()))).first)).second;
}

template<typename Key, typename T, class Compare, class Allocator>
bool operator== (const TRBTree<Key, T, Compare, Allocator>& lhs, const TRBTree<Key, T, Compare, Allocator>& rhs);

template<typename Key, typename T, class Compare, class Allocator>
bool operator!= (const TRBTree<Key, T, Compare, Allocator>& lhs, const TRBTree<Key, T, Compare, Allocator>& rhs);

template<typename Key, typename T, class Compare, class Allocator>
bool operator< (const TRBTree<Key, T, Compare, Allocator>& lhs, const TRBTree<Key, T, Compare, Allocator>& rhs);

template<typename Key, typename T, class Compare, class Allocator>
bool operator<= (const TRBTree<Key, T, Compare, Allocator>& lhs, const TRBTree<Key, T, Compare, Allocator>& rhs);

template<typename Key, typename T, class Compare, class Allocator>
bool operator> (const TRBTree<Key, T, Compare, Allocator>& lhs, const TRBTree<Key, T, Compare, Allocator>& rhs);

template<typename Key, typename T, class Compare, class Allocator>
bool operator>= (const TRBTree<Key, T, Compare, Allocator>& lhs, const TRBTree<Key, T, Compare, Allocator>& rhs);

/* Table traverser functions.
void trb_t_init (struct trb_traverser *, struct trb_table *);
void *trb_t_first (struct trb_traverser *, struct trb_table *);
void *trb_t_last (struct trb_traverser *, struct trb_table *);
void *trb_t_find (struct trb_traverser *, struct trb_table *, void *);
void *trb_t_insert (struct trb_traverser *, struct trb_table *, void *);
void *trb_t_copy (struct trb_traverser *, const struct trb_traverser *);
void *trb_t_next (struct trb_traverser *);
void *trb_t_prev (struct trb_traverser *);
void *trb_t_cur (struct trb_traverser *);
void *trb_t_replace (struct trb_traverser *, void *);
*/


#endif /* TRBTREE_HH_ */