#ifndef __SMOOTHSORT_H_
#define __SMOOTHSORT_H_

#include <assert.h>
#include <algorithm>

template<class T>
void smoothsort(T* data, size_t N);     // Sort an array, swapping elements in place

// Assumes 64-bit machine. This should also work for 32-bit machines, but for larger architectures,
// the array should (in theory) be expanded. However, it is doubtful that more than 2^64-1 items
// will ever be thrown at this algorithm.
const size_t Leonardo_k[] = {1, 1, 3, 5, 9, 15, 25, 41, 67, 109, 177, 287, 465, 753, 1219,
                             1973, 3193, 5167, 8361, 13529, 21891, 35421u, 57313u, 92735u,
                 150049u, 242785u, 392835u, 635621u, 1028457u, 1664079u, 2692537u,
                 4356617u, 7049155u, 11405773u, 18454929u, 29860703u, 48315633u,
                 78176337u, 126491971u, 204668309u, 331160281u, 535828591u, 866988873u,
                 1402817465u, 2269806339u, 3672623805u, 5942430145u, 9615053951u,
                 15557484097u, 25172538049u, 40730022147u, 65902560197u, 106632582345u,
                 172535142543u, 279167724889u, 451702867433u, 730870592323u,
                 1182573459757u, 1913444052081u, 3096017511839u, 5009461563921u,
                 8105479075761u, 13114940639683u, 21220419715445u, 34335360355129u,
                 55555780070575u, 89891140425705u, 145446920496281u, 235338060921987u,
                 380784981418269u, 616123042340257u, 996908023758527u, 1613031066098785u,
                 2609939089857313u, 4222970155956099u, 6832909245813413u,
                 11055879401769513u, 17888788647582927u, 28944668049352441u,
                 46833456696935369u, 75778124746287811u, 122611581443223181u,
                 198389706189510993u, 321001287632734175u, 519390993822245169u,
                 840392281454979345u, 1359783275277224515u, 2200175556732203861u,
                 3559958832009428377u, 5760134388741632239u, 9320093220751060617u,
                 15080227609492692857u};


// Leonardo heap class, similar to a binary heap, and required for the smoothsort algorithm
template<class T>
class TLeonardoHeap
{
    T* data;    // Array of pointers to the elements to be sorted
    size_t N;   // Length of <data> array
    size_t tree_vector; // Bitvector, where bit k marks the presence or absence of Lt_{k+m} in the heap, where m denotes <first_tree>
    unsigned short int first_tree;      // Order of rightmost tree in heap
public:
    TLeonardoHeap(T* _data, size_t _N);
    ~TLeonardoHeap();
    void insertion_sort(size_t element);        // Insert <element> into the heap. <element> is assumed to be first element in <data> not yet incorporated
    void dequeue_max(size_t element);   // Pop off the rightmost element in the heap and rebalance
private:
    void heapify(size_t root, size_t order);                            // Restore the max-heap property in the given tree
    void filter(size_t element, size_t order, bool test_children = true); // Restore ascending root and max-heap properties leftward from <element>
    void swap(size_t element_1, size_t element_2);                              // Swap two elements in <data> array
};

/** ////////////////////////////////////////////////////////////////////////////////////////////
 // Leonardo heap member functions
 //////////////////////////////////////////////////////////////////////////////////////////////*/

// Constructor
template<class T>
TLeonardoHeap<T>::TLeonardoHeap(T* _data, size_t _N) :
                data(_data), N(_N)
{
    // Initialize the heap with the first two elements
    tree_vector = 3;
    first_tree = 0;
    if(N > 1)
    {
        if(data[0] > data[1])
        {
            swap(0, 1);
        }
    }
    // Insert each element sequentially
    for(size_t i = 2; i < N; i++)
    {
        insertion_sort(i);
    }
}

// Destructor
template<class T>
TLeonardoHeap<T>::~TLeonardoHeap()
{
}

// Insert <element> into the heap. <element> is assumed to be first element in <data> not yet incorporated
template<class T>
inline void TLeonardoHeap<T>::insertion_sort(size_t element)
{
    if((tree_vector & 1) && (tree_vector & 2))
    {        // Smallest two trees are of sequential order
        // Insert a tree of order L_{k+2} with the new element as the root
        tree_vector = (tree_vector >> 2) | 1;
        first_tree += 2;
    }
    else if(first_tree == 1)
    {
        // Insert a singleton node of order L_0
        tree_vector = (tree_vector << 1) | 1;
        first_tree = 0;
    }
    else
    {
        // Insert a singleton node of order L_1
        tree_vector = (tree_vector << (first_tree - 1)) | 1;
        first_tree = 1;
    }
    // Filter leftwards to restore ascending root and max-heap properties
    filter(element, first_tree);
}

// Pop off the rightmost element in the heap and rebalance
template<class T>
inline void TLeonardoHeap<T>::dequeue_max(size_t element)
{
    if(first_tree >= 2)
    {       // Root is of order greater than two
        // Expose the two child nodes as tree roots
        // Restore the ascending root and max-heap properties of the exposed tree to the left
        tree_vector = (tree_vector << 1) ^ 3;   // w1 -> w01
        first_tree -= 2;
        filter(element - Leonardo_k[first_tree] - 1, first_tree + 1, false);
        // Restore the ascending root and max-heap properties of the exposed tree to the right
        tree_vector = (tree_vector << 1) | 1;   // w01 -> w011
        filter(element - 1, first_tree, false);
    }
    else if(__builtin_expect(!!(first_tree == 0), 0))
    {
        // Remove the Lt_0 tree, leaving the Lt_1 tree on the right
        tree_vector >>= 1;
        first_tree = 1;
    }
    else
    {    // The rightmost root is of order 1
        // Search for the next tree
        tree_vector >>= 1;
        first_tree++;
        for(; first_tree < N + 1; first_tree++, tree_vector >>= 1)
        {
            if(tree_vector & 1)
            {
                break;
            }
        }
    }
}

// Restore ascending root and max-heap properties leftward from <element>
template<class T>
inline void TLeonardoHeap<T>::filter(size_t element, size_t order, bool test_children)
{
    size_t current = element;
    size_t order_current = order;
    size_t size_current;
    size_t bitvector_mask = 2;
    while(true)
    {
        // Check that there is a tree to the left
        size_current = Leonardo_k[order_current];
        if(size_current > current)
        {
            break;
        }
        // Determine whether root needs to be swapped with next tree to the left
        if(!(data[current - size_current] > data[current]))
        {
            break;
        }
        else if((size_current == 1) || !test_children)
        {      // Root of next tree greater than root of current tree
            swap(current, current - size_current);        // Singleton node, or current tree is already heapified
        }
        else
        {        // Current tree not singleton node and child nodes must be compared
            // Root of next tree greater than both children of root of current tree
            if((data[current - size_current] > data[current - 1]) && (data[current - size_current] > data[current - 1 - Leonardo_k[order_current - 2]]))
            {
                swap(current, current - size_current);
            }
            else
            {
                break;
            }
        }
        // Find the order of the next tree to the left
        order_current++;
        for(; order_current < N + 1; order_current++, bitvector_mask <<= 1)
        { // For is used just for the purposes of safety. A while loop would work too.
            if(tree_vector & bitvector_mask)
            {
                bitvector_mask <<= 1;
                break;
            }
        }
        // Shift the position marker leftwards
        current -= size_current;
    }
    heapify(current, order_current);
}

// Restore the max-heap property in the given tree
template<class T>
inline void TLeonardoHeap<T>::heapify(size_t root, size_t order)
{
    size_t comp, comp_order;
    while(true)
    {
        if(order <= 1)
        {
            break;
        }       // Break if root is a singleton node
        // Determine which of the two children is greater
        if(__builtin_expect(!!(data[root - 1] > data[root - 1 - Leonardo_k[order - 2]]), 1))
        {
            comp = root - 1;
            comp_order = order - 2;
        }
        else
        {
            comp = root - 1 - Leonardo_k[order - 2];
            comp_order = order - 1;
        }
        // Compare the root with the greater of the two children
        if(data[comp] > data[root])
        {
            swap(root, comp);
        }
        else
        {
            break;
        }
        // shift the root downwards
        root = comp;
        order = comp_order;
    }
}

// Swap two elements in <data> array
template<class T>
inline void TLeonardoHeap<T>::swap(size_t element_1, size_t element_2)
{
    std::swap(data[element_1],data[element_2]);
}

/** ////////////////////////////////////////////////////////////////////////////////////////////
 // Smoothsort implementations
 //////////////////////////////////////////////////////////////////////////////////////////////*/

// Sort an array, swapping elements in place
template<class T>
void smoothsort(T* data, size_t N)
{
    TLeonardoHeap<T> lh(data, N);
    for(size_t i = 1; i < N - 1; i++)
    {
        lh.dequeue_max(N - i);
    }
}

#endif // __SMOOTHSORT_H_
