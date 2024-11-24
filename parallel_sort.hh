#pragma once

#include <vector>
#include <algorithm>
#include <queue>
#include <thread>
#include <mutex> 
#include <condition_variable>

/* (C) Ilia Kaliuzhnyi 2024

  Contains two algorithms for parallel sorting. The latter, "vector_parallel_sort"
  divides range into number of regions equal to hardware_concurrency, uses std::sort
  to sort each region and afterwards uses std::inplace_merge to merge sorted regions.
  This happily consumes up to x2 memory during inplace_merge

  The former, "ugh_sort_parallel" is an implementation of parallel in-place introsort with 
  algorithms taken directly from wikipedia. Pivot selection is "best of 5". This has 
  almost the same speed as the above, and uses no extra memory, but is a lot more code.
  Insertion or heap sorts are available on last stage.
  Introsort itself is ~10% slower than std::sort

  On 16 core AMD CPU speedup is roughly x5.85 for the former sort. */

// QSORT
// // Sorts a (portion of an) array, divides it into partitions, then sorts those
// algorithm quicksort(A, lo, hi) is 
//   if lo >= 0 && hi >= 0 && lo < hi then
//     p := partition(A, lo, hi) 
//     quicksort(A, lo, p) // Note: the pivot is now included
//     quicksort(A, p + 1, hi) 
// // Divides array into two partitions
// algorithm partition(A, lo, hi) is 
//   // Pivot value
//   pivot := A[lo] // Choose the first element as the pivot
//   // Left index
//   i := lo - 1 
//   // Right index
//   j := hi + 1
//   loop forever 
//     // Move the left index to the right at least once and while the element at
//     // the left index is less than the pivot
//     do i := i + 1 while A[i] < pivot
//     // Move the right index to the left at least once and while the element at
//     // the right index is greater than the pivot
//     do j := j - 1 while A[j] > pivot
//     // If the indices crossed, return
//     if i >= j then return j
//     // Swap the elements at the left and right indices
//     swap A[i] with A[j]

// INSERTION SORT
// i ← 1
// while i < length(A)
//     j ← i
//     while j > 0 and A[j-1] > A[j]
//         swap A[j] and A[j-1]
//         j ← j - 1
//     end while
//     i ← i + 1
// end while

// HEAP SORT
// procedure heapsort(a, count) is
//     input: an unordered array a of length count
//     start ← floor(count/2)
//     end ← count
//     while end > 1 do
//         if start > 0 then    (Heap construction)
//             start ← start − 1
//         else                 (Heap extraction)
//             end ← end − 1
//             swap(a[end], a[0])
//         (The following is siftDown(a, start, end))
//         root ← start
//         while iLeftChild(root) < end do
//             child ← iLeftChild(root)
//             (If there is a right child and that child is greater)
//             if child+1 < end and a[child] < a[child+1] then
//                 child ← child + 1
//             if a[root] < a[child] then
//                 swap(a[root], a[child])
//                 root ← child         (repeat to continue sifting down the child now)
//             else
//                 break                (return to outer loop)


template<typename T> 
void ugh_swap_if_greater(T& lhs, T& rhs) noexcept
{
    if (lhs > rhs)
        std::swap(lhs, rhs);
}

// Below code needs profiling
// // For all types except integral types:
// template<typename T>
// std::enable_if_t<!std::is_integral_v<T>> ugh_swap_if_greater(T& lhs, T& rhs) noexcept
// {
//     if (lhs > rhs)
//         std::swap(lhs, rhs);
// }

// // For integral types only:
// template<typename T>
// std::enable_if_t<std::is_integral_v<T>> ugh_swap_if_greater(T& lhs, T& rhs) noexcept
// {
//     T tlhs = rhs ^ ((lhs ^ rhs) & - (lhs < rhs)); // bit twiddling hacks min(x, y)
//     T trhs = lhs ^ ((lhs ^ rhs) & - (lhs < rhs)); // bit twiddling hacks max(x, y)
//     lhs = tlhs;
//     rhs = trhs;
// }

template <typename T>
size_t ugh_qsort_partition(std::vector<T>& to_sort, size_t lo, size_t hi)
{
  // best of 5 pivot is still quite bad
  // since we're reaching for memory anyway 
  // we can sort 5 caheline sized regions to take 
  // better mean

  const size_t mid = lo + ((hi - lo) >> 1);
  const size_t lmid = lo + ((mid - lo) >> 1);
  const size_t rmid = mid + ((hi - mid) >> 1);

  ugh_swap_if_greater(to_sort[lo], to_sort[lmid]);
  ugh_swap_if_greater(to_sort[rmid], to_sort[hi]);
  ugh_swap_if_greater(to_sort[mid], to_sort[hi]);
  ugh_swap_if_greater(to_sort[mid], to_sort[rmid]);
  ugh_swap_if_greater(to_sort[lo], to_sort[rmid]);
  ugh_swap_if_greater(to_sort[lo], to_sort[mid]);
  ugh_swap_if_greater(to_sort[lmid], to_sort[hi]);
  ugh_swap_if_greater(to_sort[lmid], to_sort[rmid]);
  ugh_swap_if_greater(to_sort[lmid], to_sort[mid]);

  T pivot = to_sort[mid];

  size_t i = lo - 1;
  size_t j = hi + 1;
  while(true) 
  {
    // code below basically does this, but in a more optimized way
    // do i+=1; while(to_sort[i] < pivot);
    // do j-=1; while(to_sort[j] > pivot);
    ++i;
    --j;
    while (true) 
    {
      bool changed = false;
      if (to_sort[i] < pivot) 
      {
        changed = true;
        ++i;
      }
      if (to_sort[j] > pivot)
      {
        changed = true;
        --j;
      }
      if (!changed)
        break;
    }
    if (i >= j)
      return j;
    std::swap(to_sort[i], to_sort[j]);
  }
}

template <typename T>
void ugh_qsort_heap(std::vector<T>& to_sort, size_t lo, size_t hi)
{
    int count = hi - lo + 1;
    size_t start = count >> 1;
    size_t end = count;
    while (end > 1) 
    {
      if (start > 0) 
        --start;
      else 
      {
        --end;
        std::swap(to_sort[lo + end], to_sort[lo]);
      }
      size_t root = start;
      while(2*root + 1 < end) 
      {
        size_t child = 2*root + 1;
        if (child + 1 < end && to_sort[lo + child] < to_sort[lo +child+1]) 
          ++child;
        
        if (to_sort[lo + root] < to_sort[lo + child]) 
        {
          std::swap(to_sort[lo + root], to_sort[lo + child]);
          root = child;
        }
        else
          break;
      }
    }
}

template <typename T>
void ugh_qsort_insertion(std::vector<T>& to_sort, size_t lo, size_t hi)
{
  size_t i = 1;
  while (lo + i <= hi) 
  {
    size_t j = i;
    while (j > 0 && to_sort[lo + j-1] > to_sort[lo + j])
    {
      std::swap(to_sort[lo + j-1], to_sort[lo + j]);
      --j;
    }
    ++i;
  }
}

template <typename T>
void ugh_qsort(std::vector<T>& to_sort, size_t lo, size_t hi)
{
  if (lo >= 0 && hi >= 0 && lo < hi) 
  {
    if ((sizeof(T)*(hi - lo + 1)) <= 64) // cacheline
    {
      ugh_qsort_heap(to_sort, lo, hi);
    }
    else 
    {
      size_t p = ugh_qsort_partition(to_sort,lo,hi);
      ugh_qsort(to_sort, lo, p);
      ugh_qsort(to_sort, p+1, hi);
    }
  }
}

template <typename T>
inline void ugh_sort(std::vector<T>& to_sort)
{
  if (to_sort.empty())
    return;
  ugh_qsort(to_sort, 0, to_sort.size()-1);
}

template <typename T>
void ugh_qsort_parallel(std::vector<T>& to_sort, std::deque<std::pair<size_t, size_t>>& tasks, std::mutex& mx, std::condition_variable& cv, size_t& latch)
{
  while (true) 
  {
    std::pair<size_t, size_t> work;
    {
      std::unique_lock<std::mutex> guard(mx); // lock mutex
      if(tasks.empty()) 
      {
        if(!latch) 
        {
          cv.notify_all();
          return;
        }
        cv.wait(guard); // wait until there's stuff in the queue
      }
      if (!tasks.empty())
      {
        ++latch;
        work = tasks.front(); // take a bit of work
        tasks.pop_front();
        if (!tasks.empty()) // still more work, wake someone up
          cv.notify_one(); 
      }
      else if (tasks.empty())
      {
        if (!latch) 
        {
          // no active threads and empty queue, time to exit
          cv.notify_all();
          return;
        }
      }
    }
    size_t lo = work.first;
    size_t hi = work.second;
    if ((sizeof(T)*(hi - lo + 1)) <= 1024*1024) // typical L2 size
    {
      ugh_qsort(to_sort, lo, hi);
      {
        std::unique_lock<std::mutex> guard(mx);
        if (latch)
          --latch;
      }
    }
    else 
    {
      size_t p = ugh_qsort_partition(to_sort,lo,hi);
      {
        std::unique_lock<std::mutex> guard(mx);
        if (p > (hi - lo)>>2) // work next on smaller part, let others deal with bigger 
        {
          tasks.push_front({p+1, hi});
          tasks.push_back({lo, p});
        }
        else 
        {
          tasks.push_back({p+1, hi});
          tasks.push_front({lo, p});
        }
        if (latch)
          --latch;
      }
    }
  }
}

template <typename T>
inline void ugh_sort_parallel(std::vector<T>& to_sort, size_t num_threads = 0/*0 means all available cores*/)
{
  // set up thread pool, queue and synchronization
  if (!num_threads)
    num_threads = std::thread::hardware_concurrency();
  size_t latch = 0; // latch is number of active threads
  // start all threads push array into the queue
  std::vector<std::shared_ptr<std::thread>> workers;
  std::deque<std::pair<size_t, size_t>> tasks;
  std::mutex mx;
  std::condition_variable cv;
  tasks.push_front({0, to_sort.size()-1});
  for (size_t i = 0; i < num_threads; ++i)
      workers.emplace_back(new std::thread([&](){ ugh_qsort_parallel(to_sort, tasks, mx, cv, latch); }));
  // join threads
  for (auto& worker : workers)
      worker->join();
  workers.clear();
}
