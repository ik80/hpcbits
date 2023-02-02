#pragma once

// multithreaded sorting using C++11
template <typename T>
void vector_parallel_sort(std::vector<T>& to_sort)
{
    // merger must be kept sorted in descedning order
    std::vector<std::pair<typename std::vector<T>::iterator, typename std::vector<T>::iterator>> regions;

    size_t numRegions = std::thread::hardware_concurrency();
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