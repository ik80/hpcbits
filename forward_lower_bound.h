#pragma once

#include <algorithm>

// use this to search in a sorted range when you know the value must be close to the start
template<class ForwardIt, class T>
ForwardIt my_forward_lower_bound(ForwardIt first, ForwardIt last, const T& value)
{
    typename std::iterator_traits<ForwardIt>::difference_type step = 1, max_distance = std::distance(first, last);
    if (first == last)
        return last;
    if (value < *first)
        return first;
    ForwardIt prev = first;
    ForwardIt it = prev;
    while (step < max_distance)
    {
        std::advance(it, step);
        max_distance -= step;
        if (*prev <= value && value <= *it)
            return std::lower_bound(prev, it, value);
        prev = it;
        step = step << 1;
    }
    if (prev != last)
        return std::lower_bound(prev, last, value);
    return last;
}