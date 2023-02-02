#include <algorithm>

//Below code works wonders when you need to intersect two sets one if which is much larger than the other.
//For example for on random data sets of 1024x size difference, speedup compared to std::set_intersect is 3x times.
//Intersecting equal sized but highly distinct sets slows down compared to std::set_intersect only by about 15%

template<class ForwardIterator, class T>
inline ForwardIterator my_lower_bound (ForwardIterator first, ForwardIterator last, const T& val)
{
    ForwardIterator it;
    size_t count, step;
    count = distance (first, last);
    while (count > 0)
    {
        it = first;
        step = count / 2;
        advance (it, step);
        if (*it < val)
        {
            first = ++it;
            count -= step + 1;
        }
        else
            count = step;
    }
    return first;
}

template<class InputIterator1, class InputIterator2, class OutputIterator>
OutputIterator smart_set_intersection (InputIterator1 first1, InputIterator1 last1, InputIterator2 first2, InputIterator2 last2, OutputIterator result)
{
    while (first1 != last1 && first2 != last2)
    {
        size_t stepSize = 1;
        if (*first1 < *first2)
        {
            InputIterator1 prevPos = first1;
            std::advance(first1, stepSize);
            while(*first1 < *first2)
            {
                stepSize *= 2;
                prevPos = first1;
                std::advance(first1, stepSize);
                if (first1 > last1)
                {
                    first1 = last1;
                    break;
                }
            }
            first1 = my_lower_bound(prevPos, first1, *first2);
        }
        else if (*first2 < *first1)
        {
            InputIterator1 prevPos = first2;
            std::advance(first2, stepSize);
            while(*first2 < *first1)
            {
                stepSize *= 2;
                prevPos = first2;
                std::advance(first2, stepSize);
                if (first2 > last2)
                {
                    first2 = last2;
                    break;
                }
            }
            first2 = my_lower_bound(prevPos, first2, *first1);
        }
        else
        {
            *result = *first1;
            ++result;
            ++first1;
            ++first2;
        }
    }
    return result;
}

template<class InputIterator1, class InputIterator2, class OutputIterator>
OutputIterator linear_set_intersection (InputIterator1 first1, InputIterator1 last1, InputIterator2 first2, InputIterator2 last2, OutputIterator result)
{
    while (first1 != last1 && first2 != last2)
    {
        if (*first1 < *first2)
        {
            ++first1;
        }
        else if (*first2 < *first1)
        {
            ++first2;
        }
        else
        {
            *result = *first1;
            ++result;
            ++first1;
            ++first2;
        }
    }
    return result;
}

// test code below
/*
int main (int argc, char* argv[])
{
    srand(time(0));
    {
        std::cout << "Random case" << std::endl;
        std::vector<size_t> vec1;
        vec1.reserve(1024*1024*32);
        std::vector<size_t> vec2;
        vec2.reserve(1024*1024*32);
        std::vector<size_t> res;
        res.reserve(1024*1024*32);
        size_t counter1 = 0;
        for (size_t i = 0; i < 1024*1024*32; ++i)
        {
            ++counter1;
            vec1.push_back(counter1);
        }
        for (size_t i = 0; i < 1024*32; ++i)
        {
            vec2.push_back(rand()%(1024*1024*32));
        }
        std::sort(vec1.begin(), vec1.end());
        std::sort(vec2.begin(), vec2.end());
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            std::set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "stl " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            linear_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "linear " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            smart_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "smart " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
    }
    {
        std::cout << "Assymetric case" << std::endl;
        std::vector<size_t> vec1;
        vec1.reserve(1024*1024*32);
        std::vector<size_t> vec2;
        vec2.reserve(1024*1024*32);
        std::vector<size_t> res;
        //res.reserve(1024*1024*32);
        size_t counter1 = 0;
        size_t counter2 = 0;
        for (size_t i = 0; i < 1024*1024*32; ++i)
        {
            ++counter1;
            vec1.push_back(counter1);
        }
        for (size_t i = 0; i < 1024*32; ++i)
        {
            counter2 += 1024;
            vec2.push_back(counter2);
        }
        std::sort(vec1.begin(), vec1.end());
        std::sort(vec2.begin(), vec2.end());
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            std::set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "stl " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            linear_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "linear " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            smart_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "smart " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
    }
    {
        std::cout << "Dense case" << std::endl;
        std::vector<size_t> vec1;
        vec1.reserve(1024*1024*32);
        std::vector<size_t> vec2;
        vec2.reserve(1024*1024*32);
        std::vector<size_t> res;
        res.reserve(1024*1024*32);
        size_t counter1 = 0;
        size_t counter2 = 0;
        for (size_t i = 0; i < 1024*1024*32; ++i)
        {
            ++counter1;
            ++counter2;
            vec1.push_back(counter1);
            vec2.push_back(counter2);
        }
        std::sort(vec1.begin(), vec1.end());
        std::sort(vec2.begin(), vec2.end());
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            std::set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "stl " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            linear_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "linear " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            smart_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "smart " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
    }
    {
        std::cout << "Sparse case" << std::endl;
        std::vector<size_t> vec1;
        vec1.reserve(1024*1024*32);
        std::vector<size_t> vec2;
        vec2.reserve(1024*1024*32);
        std::vector<size_t> res;
        res.reserve(1024*1024*32);
        size_t counter1 = 0;
        size_t counter2 = 0;
        for (size_t i = 0; i < 1024*1024*32; ++i)
        {
            counter1 += 1 + rand()%1000;
            counter2 += 1 + rand()%1000;
            vec1.push_back(counter1);
            vec2.push_back(counter2);
        }
        std::sort(vec1.begin(), vec1.end());
        std::sort(vec2.begin(), vec2.end());
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            std::set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "stl " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            linear_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "linear " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
        res.clear();
        {
            boost::timer::cpu_timer theTimer;
            theTimer.start();
            smart_set_intersection(vec1.begin(), vec1.end(), vec2.begin(), vec2.end(), std::back_inserter(res));
            theTimer.stop();
            double secondspassed = (double) theTimer.elapsed().wall / 1000000000LL;
            double opspersecond = ((double) (1024*1024*32)) / secondspassed;
            std::cout << "smart " << secondspassed << " seconds, " << std::setprecision(15) << opspersecond << " elements persecond, intersection size " << res.size() << ", capacity "<< res.capacity() << std::endl;
        }
    }
    exit(0);
}*/