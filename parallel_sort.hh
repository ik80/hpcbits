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

  The former, "ugh_sort_parallel" is an implementation of parallel introsort with 
  algorithms taken directly from wikipedia. Pivot selection is "best of 5". This has 
  almost the same speed as the above, and uses no extra memory, but is a lot more code.
  Introsort itself is ~10% slower than std::sort.

  On 16 core AMD CPU speedup is roughly x5.85 for the former sort. */

template<typename T>
void __attribute__((always_inline)) ugh_swap_if_less(T* lhs, T* rhs) noexcept
{
    if (*lhs > *rhs)
        std::swap(*lhs, *rhs);
}

template <typename T>
size_t ugh_qsort_partition(std::vector<T>& to_sort, size_t lo, size_t hi)
{
  // best of 5 pivot
  const size_t mid = lo + ((hi - lo) >> 1);
  const size_t lmid = lo + ((mid - lo) >> 1);
  const size_t rmid = mid + ((hi - mid) >> 1);
  ugh_swap_if_less(&(to_sort[lo]), &(to_sort[lmid]));
  ugh_swap_if_less(&(to_sort[rmid]), &(to_sort[hi]));
  ugh_swap_if_less(&(to_sort[mid]), &(to_sort[hi]));
  ugh_swap_if_less(&(to_sort[mid]), &(to_sort[rmid]));
  ugh_swap_if_less(&(to_sort[lo]), &(to_sort[rmid]));
  ugh_swap_if_less(&(to_sort[lo]), &(to_sort[mid]));
  ugh_swap_if_less(&(to_sort[lmid]), &(to_sort[hi]));
  ugh_swap_if_less(&(to_sort[lmid]), &(to_sort[rmid]));
  ugh_swap_if_less(&(to_sort[lmid]), &(to_sort[mid]));
  T pivot = to_sort[mid];

  size_t i = lo - 1;
  size_t j = hi + 1;
  while(true) 
  {
    do i+=1; while(to_sort[i] < pivot);
    do j-=1; while(to_sort[j] > pivot);
    if (i >= j)
      return j;
    std::swap(to_sort[i], to_sort[j]);
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
      ugh_qsort_insertion(to_sort, lo, hi);
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
    if (lo >= 0 && hi >= 0 && lo < hi) 
    {
      if (hi - lo < (to_sort.size() / 32)) 
      {
        ugh_qsort(to_sort, lo, hi);
        {
          std::unique_lock<std::mutex> guard(mx);
          if (latch)
            --latch;
        }
      }
      else if ((sizeof(T)*(hi - lo + 1)) <= 64) // cacheline 
      {
        ugh_qsort_insertion(to_sort, lo, hi);
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
          // let partition steps be the first and insertsort last 
          if ((sizeof(T)*(hi - p)) <= 64) 
            tasks.push_front({p+1, hi});  
          else
            tasks.push_back({p+1, hi});
          if ((sizeof(T)*(p - lo)) <= 64)
            tasks.push_front({lo, p});  
          else
            tasks.push_back({lo, p});
          cv.notify_one();
          if (latch)
            --latch;
        }
      }
    }
    else
    {
      std::unique_lock<std::mutex> guard(mx);
      if (latch)
        --latch;
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

// merge sorted ranges in place
template <typename T>
void vector_parallel_sort(std::vector<T>& to_sort, int threads)
{
    // merger must be kept sorted in descedning order
    std::vector<std::pair<typename std::vector<T>::iterator, typename std::vector<T>::iterator>> regions;

    size_t numRegions = std::thread::hardware_concurrency();
    if (threads) numRegions = threads;
    size_t regionSize = to_sort.size() / numRegions;

    std::vector<std::shared_ptr<std::thread>> workers;
    workers.reserve(numRegions + 1); // haha cant touch this 
    regions.reserve(numRegions + 1); // haha cant touch this 

    for (size_t region = 0; region < numRegions; ++region)
        regions.push_back(std::make_pair(to_sort.begin() + region * regionSize, to_sort.begin() + (region + 1) * regionSize));
    if (to_sort.size() % regionSize)
        regions.back().second = to_sort.end();

    for (auto& region : regions)
        workers.emplace_back(new std::thread([&]() {std::sort(region.first, region.second); }));
    for (auto& worker : workers)
        worker->join();
    workers.clear();

    while (regions.size() > 1)
    {
        std::vector<std::pair<typename std::vector<T>::iterator, typename std::vector<T>::iterator>> nextRegions;
        for (size_t i = 0; i < regions.size() - 1; i += 2)
        {
            workers.emplace_back(new std::thread([&regions, i]() {std::inplace_merge(regions[i].first, regions[i].second, regions[i + 1].second); }));
            nextRegions.push_back(std::make_pair(regions[i].first, regions[i + 1].second));
        }
        if (regions.size() % 2)
            nextRegions.push_back(std::make_pair(regions.back().first, regions.back().second));
        for (auto& worker : workers)
            worker->join();
        workers.clear();
        regions = nextRegions;
    }
}
