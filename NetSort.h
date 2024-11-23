#pragma once

// Sorting Networks generator by http://jgamble.ripco.net/cgi-bin/nw.cgi?inputs=32&algorithm=best&output=svg

// More indepth description and best specimen https://bertdobbelaere.github.io/sorting_networks.html

namespace
{
    template <typename T>
    struct stack_node
    {
        T *lo;
        T *hi;
    };
    template <typename T>
    inline void __attribute__((always_inline)) stack_init(stack_node<T> * & top) noexcept
    {
        top->lo = 0;
        top->hi = 0;
        ++top;
    }
    template <typename T>
    inline void __attribute__((always_inline)) stack_push(T * & low, T * & high, stack_node<T> * & top) noexcept
    {
        top->lo = low;
        top->hi = high;
        ++top;
    }
    template <typename T>
    inline void __attribute__((always_inline)) stack_pop(T * & low, T * & high, stack_node<T> * & top) noexcept
    {
        --top;
        low = top->lo;
        high = top->hi;
    }
    template <typename T>
    inline bool __attribute__((always_inline)) stack_not_empty(stack_node<T> * stack, stack_node<T> * top) noexcept
    {
        return stack < top;
    }

    // For all types except integral types:
    template<typename T>
    typename std::enable_if<!std::is_integral<T>::value>::type __attribute__((always_inline)) NetSortSwapIfLess(T* lhs, T* rhs) noexcept
    {
        if (*lhs > *rhs)
            std::swap(*lhs, *rhs);
    }

    // For integral types only:
    template<typename T>
    typename std::enable_if<std::is_integral<T>::value>::type __attribute__((always_inline)) NetSortSwapIfLess(T* lhs, T* rhs) noexcept
    {
        T tlhs = *rhs ^ ((*lhs ^ *rhs) & - (*lhs < *rhs)); // bit twiddling hacks min(x, y)
        T trhs = *lhs ^ ((*lhs ^ *rhs) & - (*lhs < *rhs)); // bit twiddling hacks max(x, y)
        *lhs = tlhs;
        *rhs = trhs;
    }

    template <typename T>
    inline void NetSort16(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[10]), &(arr[11]));
        NetSortSwapIfLess(&(arr[12]), &(arr[13]));
        NetSortSwapIfLess(&(arr[14]), &(arr[15]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[8]), &(arr[10]));
        NetSortSwapIfLess(&(arr[12]), &(arr[14]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[9]), &(arr[11]));
        NetSortSwapIfLess(&(arr[13]), &(arr[15]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[8]), &(arr[12]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[9]), &(arr[13]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[10]), &(arr[14]));
        NetSortSwapIfLess(&(arr[3]), &(arr[7]));
        NetSortSwapIfLess(&(arr[11]), &(arr[15]));
        NetSortSwapIfLess(&(arr[0]), &(arr[8]));
        NetSortSwapIfLess(&(arr[1]), &(arr[9]));
        NetSortSwapIfLess(&(arr[2]), &(arr[10]));
        NetSortSwapIfLess(&(arr[3]), &(arr[11]));
        NetSortSwapIfLess(&(arr[4]), &(arr[12]));
        NetSortSwapIfLess(&(arr[5]), &(arr[13]));
        NetSortSwapIfLess(&(arr[6]), &(arr[14]));
        NetSortSwapIfLess(&(arr[7]), &(arr[15]));
        NetSortSwapIfLess(&(arr[5]), &(arr[10]));
        NetSortSwapIfLess(&(arr[6]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[12]));
        NetSortSwapIfLess(&(arr[13]), &(arr[14]));
        NetSortSwapIfLess(&(arr[7]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[8]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[13]));
        NetSortSwapIfLess(&(arr[2]), &(arr[8]));
        NetSortSwapIfLess(&(arr[11]), &(arr[14]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[11]), &(arr[13]));
        NetSortSwapIfLess(&(arr[3]), &(arr[8]));
        NetSortSwapIfLess(&(arr[7]), &(arr[12]));
        NetSortSwapIfLess(&(arr[6]), &(arr[8]));
        NetSortSwapIfLess(&(arr[10]), &(arr[12]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[7]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[11]), &(arr[12]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
    }

    template <typename T>
    inline void NetSort15(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[10]), &(arr[11]));
        NetSortSwapIfLess(&(arr[12]), &(arr[13]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[8]), &(arr[10]));
        NetSortSwapIfLess(&(arr[12]), &(arr[14]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[9]), &(arr[11]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[8]), &(arr[12]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[9]), &(arr[13]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[10]), &(arr[14]));
        NetSortSwapIfLess(&(arr[3]), &(arr[7]));
        NetSortSwapIfLess(&(arr[0]), &(arr[8]));
        NetSortSwapIfLess(&(arr[1]), &(arr[9]));
        NetSortSwapIfLess(&(arr[2]), &(arr[10]));
        NetSortSwapIfLess(&(arr[3]), &(arr[11]));
        NetSortSwapIfLess(&(arr[4]), &(arr[12]));
        NetSortSwapIfLess(&(arr[5]), &(arr[13]));
        NetSortSwapIfLess(&(arr[6]), &(arr[14]));
        NetSortSwapIfLess(&(arr[5]), &(arr[10]));
        NetSortSwapIfLess(&(arr[6]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[12]));
        NetSortSwapIfLess(&(arr[13]), &(arr[14]));
        NetSortSwapIfLess(&(arr[7]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[8]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[13]));
        NetSortSwapIfLess(&(arr[2]), &(arr[8]));
        NetSortSwapIfLess(&(arr[11]), &(arr[14]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[11]), &(arr[13]));
        NetSortSwapIfLess(&(arr[3]), &(arr[8]));
        NetSortSwapIfLess(&(arr[7]), &(arr[12]));
        NetSortSwapIfLess(&(arr[6]), &(arr[8]));
        NetSortSwapIfLess(&(arr[10]), &(arr[12]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[7]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[11]), &(arr[12]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
    }

    template <typename T>
    inline void NetSort14(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[10]), &(arr[11]));
        NetSortSwapIfLess(&(arr[12]), &(arr[13]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[8]), &(arr[10]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[9]), &(arr[11]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[8]), &(arr[12]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[9]), &(arr[13]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[3]), &(arr[7]));
        NetSortSwapIfLess(&(arr[0]), &(arr[8]));
        NetSortSwapIfLess(&(arr[1]), &(arr[9]));
        NetSortSwapIfLess(&(arr[2]), &(arr[10]));
        NetSortSwapIfLess(&(arr[3]), &(arr[11]));
        NetSortSwapIfLess(&(arr[4]), &(arr[12]));
        NetSortSwapIfLess(&(arr[5]), &(arr[13]));
        NetSortSwapIfLess(&(arr[5]), &(arr[10]));
        NetSortSwapIfLess(&(arr[6]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[12]));
        NetSortSwapIfLess(&(arr[7]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[8]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[13]));
        NetSortSwapIfLess(&(arr[2]), &(arr[8]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[11]), &(arr[13]));
        NetSortSwapIfLess(&(arr[3]), &(arr[8]));
        NetSortSwapIfLess(&(arr[7]), &(arr[12]));
        NetSortSwapIfLess(&(arr[6]), &(arr[8]));
        NetSortSwapIfLess(&(arr[10]), &(arr[12]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[7]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[11]), &(arr[12]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
    }

    template <typename T>
    inline void NetSort13(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[1]), &(arr[7]));
        NetSortSwapIfLess(&(arr[9]), &(arr[11]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[8]));
        NetSortSwapIfLess(&(arr[0]), &(arr[12]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[8]), &(arr[11]));
        NetSortSwapIfLess(&(arr[7]), &(arr[12]));
        NetSortSwapIfLess(&(arr[5]), &(arr[9]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[3]), &(arr[7]));
        NetSortSwapIfLess(&(arr[10]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[6]), &(arr[12]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
        NetSortSwapIfLess(&(arr[11]), &(arr[12]));
        NetSortSwapIfLess(&(arr[4]), &(arr[9]));
        NetSortSwapIfLess(&(arr[6]), &(arr[10]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[10]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[7]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[9]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[10]));
        NetSortSwapIfLess(&(arr[0]), &(arr[5]));
        NetSortSwapIfLess(&(arr[2]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[8]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
    }

    template <typename T>
    inline void NetSort12(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[10]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[9]), &(arr[11]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[8]), &(arr[10]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[10]));
        NetSortSwapIfLess(&(arr[5]), &(arr[9]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[10]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[11]));
        NetSortSwapIfLess(&(arr[3]), &(arr[7]));
        NetSortSwapIfLess(&(arr[4]), &(arr[8]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[11]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[10]));
        NetSortSwapIfLess(&(arr[3]), &(arr[8]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[8]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
    }

    template <typename T>
    inline void NetSort11(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[8]), &(arr[10]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[9]), &(arr[10]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[10]));
        NetSortSwapIfLess(&(arr[5]), &(arr[9]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[10]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[3]), &(arr[7]));
        NetSortSwapIfLess(&(arr[4]), &(arr[8]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[10]));
        NetSortSwapIfLess(&(arr[3]), &(arr[8]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[7]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[8]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
    }

    template <typename T>
    inline void NetSort10(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[4]), &(arr[9]));
        NetSortSwapIfLess(&(arr[3]), &(arr[8]));
        NetSortSwapIfLess(&(arr[2]), &(arr[7]));
        NetSortSwapIfLess(&(arr[1]), &(arr[6]));
        NetSortSwapIfLess(&(arr[0]), &(arr[5]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[6]), &(arr[9]));
        NetSortSwapIfLess(&(arr[0]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[8]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[3]), &(arr[6]));
        NetSortSwapIfLess(&(arr[7]), &(arr[9]));
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[8]), &(arr[9]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[2]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[8]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[4]), &(arr[7]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
    }

    template <typename T>
    inline void NetSort9(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[7]), &(arr[8]));
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[0]), &(arr[3]));
        NetSortSwapIfLess(&(arr[3]), &(arr[6]));
        NetSortSwapIfLess(&(arr[0]), &(arr[3]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[4]), &(arr[7]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[2]), &(arr[5]));
        NetSortSwapIfLess(&(arr[5]), &(arr[8]));
        NetSortSwapIfLess(&(arr[2]), &(arr[5]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
    }

    template <typename T>
    inline void NetSort8(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[6]), &(arr[7]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[5]), &(arr[7]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[3]), &(arr[7]));
        NetSortSwapIfLess(&(arr[3]), &(arr[6]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
    }

    template <typename T>
    inline void NetSort7(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[5]), &(arr[6]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[4]), &(arr[6]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[0]), &(arr[4]));
        NetSortSwapIfLess(&(arr[0]), &(arr[3]));
        NetSortSwapIfLess(&(arr[1]), &(arr[5]));
        NetSortSwapIfLess(&(arr[2]), &(arr[6]));
        NetSortSwapIfLess(&(arr[2]), &(arr[5]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
    }

    template <typename T>
    inline void NetSort6(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[4]), &(arr[5]));
        NetSortSwapIfLess(&(arr[3]), &(arr[5]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[0]), &(arr[3]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[2]), &(arr[5]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
    }

    template <typename T>
    inline void NetSort5(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[3]), &(arr[4]));
        NetSortSwapIfLess(&(arr[2]), &(arr[4]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[0]), &(arr[3]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[1]), &(arr[4]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
    }

    template <typename T>
    inline void NetSort4(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
        NetSortSwapIfLess(&(arr[2]), &(arr[3]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[1]), &(arr[3]));
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
    }

    template <typename T>
    inline void NetSort3(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[1]), &(arr[2]));
        NetSortSwapIfLess(&(arr[0]), &(arr[2]));
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
    }

    template <typename T>
    inline void NetSort2(T* arr) noexcept
    {
        NetSortSwapIfLess(&(arr[0]), &(arr[1]));
    }
}

// Sorts arrays between 2 and 16 long
template <typename T>
inline  void NetSort(T* arr, size_t arrSize) noexcept
{
    switch (arrSize)
    {
        case 2: NetSort2(arr); break;
        case 3: NetSort3(arr); break;
        case 4: NetSort4(arr); break;
        case 5: NetSort5(arr); break;
        case 6: NetSort6(arr); break;
        case 7: NetSort7(arr); break;
        case 8: NetSort8(arr); break;
        case 9: NetSort9(arr); break;
        case 10: NetSort10(arr); break;
        case 11: NetSort11(arr); break;
        case 12: NetSort12(arr); break;
        case 13: NetSort13(arr); break;
        case 14: NetSort14(arr); break;
        case 15: NetSort15(arr); break;
        case 16: NetSort16(arr); break;
        case 1: break;
        default: abort();
    }
}

/*
template <typename T>
inline void NetSort(T* arr, size_t arrSize) noexcept
{
    // would have been cool, but somehow this is slower
    static void (*pFuncs[])(T*) = {
        &NetSort2,
        &NetSort3,
        &NetSort4,
        &NetSort5,
        &NetSort6,
        &NetSort7,
        &NetSort8,
        &NetSort9,
        &NetSort10,
        &NetSort11,
        &NetSort12,
        &NetSort13,
        &NetSort14,
        &NetSort15,
        &NetSort16
    };
    (*(pFuncs[arrSize-2]))(arr);
}
*/

template<typename T>
void NetQSort(T * const pbase, size_t total_elems)
{
    // between 4 and 16, four times cacheline
    static const size_t MAX_THRESH = (64/sizeof(T))*2 < 4 ? 4 : ((64/sizeof(T))*2 > 16 ? 16 : (64/sizeof(T))*2);
    static const size_t STACK_SIZE = ( 8 * sizeof(size_t) );

    T * base_ptr = pbase;

    if (total_elems == 0)
        return;

    if (total_elems > MAX_THRESH)
    {
        T * lo = base_ptr;
        T * hi = &lo[total_elems - 1];
        stack_node<T> stack[STACK_SIZE];
        stack_node<T> * top = stack;

        stack_init(top);

        while (stack_not_empty(stack, top))
        {
            T * left_ptr;
            T * right_ptr;
            T * mid = lo + ((hi - lo)  >> 1);

            if (*((T *) mid) < *((T *) lo))
                std::swap (*((T *) mid), *((T *) lo));
            if (*((T *) hi) < *((T *) mid))
                std::swap (*((T *) hi), *((T *) mid));
            else
                goto jump_over;
            if (*((T *) mid) < *((T *) lo))
                std::swap (*((T *) mid), *((T *) lo));
            jump_over: ;

            left_ptr = lo + 1;
            right_ptr = hi - 1;

            do
            {
                while (*left_ptr < *mid)
                    ++left_ptr;

                while (*mid < *right_ptr)
                    --right_ptr;

                if (left_ptr < right_ptr)
                {
                    std::swap (*left_ptr, *right_ptr);
                    if (mid == left_ptr)
                        mid = right_ptr;
                    else if (mid == right_ptr)
                        mid = left_ptr;
                    ++left_ptr;
                    --right_ptr;
                }
                else if (left_ptr == right_ptr)
                {
                    ++left_ptr;
                    --right_ptr;
                    break;
                }
            }
            while (left_ptr <= right_ptr);

            if ((size_t) (right_ptr - lo) < MAX_THRESH)
            {
                NetSort(lo, right_ptr - lo + 1);
                if ((size_t) (hi - left_ptr) < MAX_THRESH)
                {
                    NetSort(left_ptr, hi - left_ptr + 1);
                    stack_pop(lo, hi, top);
                }
                else
                    lo = left_ptr;
            }
            else if ((size_t) (hi - left_ptr) < MAX_THRESH)
            {
                NetSort(left_ptr, hi - left_ptr + 1);
                hi = right_ptr;
            }
            else if ((right_ptr - lo) > (hi - left_ptr))
            {
                stack_push(lo, right_ptr, top);
                lo = left_ptr;
            }
            else
            {
                stack_push(left_ptr, hi, top);
                hi = right_ptr;
            }
        }
    }
    else
        NetSort(pbase, total_elems);
}
