#ifndef DYNA_WFSPSCQ_H
#define DYNA_WFSPSCQ_H

#include <atomic>

template<typename T>
class LFSPSCQueue {
    static const size_t CACHE_LINE_SIZE = 64;
    struct RingBuffer
    {
        T * ring_;
        char padding0[CACHE_LINE_SIZE - sizeof(T *)];
        std::atomic<size_t> head;
        char padding1[CACHE_LINE_SIZE - sizeof(std::atomic<size_t>)];
        std::atomic<size_t> tail;
        char padding2[CACHE_LINE_SIZE - sizeof(std::atomic<size_t>)];
    };
public:
    LFSPSCQueue(size_t buffer_size) : buffer_mask_(buffer_size - 1)
    {
        lane.ring_ = new T[buffer_size];
        lane.tail = 0;
        lane.head = 0;
    }
    ~LFSPSCQueue()
    {
        delete[] lane.ring_;
    }

    bool push(const T & value)
    {
        size_t head = lane.head.load(std::memory_order_relaxed);
        size_t next_head = next(head);
        if (next_head != lane.tail.load(std::memory_order_acquire))
        {
            lane.ring_[head] = std::move(value);
            lane.head.store(next_head, std::memory_order_release);
            return true;
        }
        return false;
    }
    bool pop(T & value)
    {
        size_t tail = lane.tail.load(std::memory_order_relaxed);
        if (tail != lane.head.load(std::memory_order_acquire))
        {
            value = std::move(lane.ring_[tail]);
            lane.tail.store(next(tail), std::memory_order_release);
            return true;
        }
        return false;
    }
private:
    inline size_t next(size_t current)
    {
        return (current + 1) & buffer_mask_;
    }
    RingBuffer lane; // lanes are cache line aligned
    size_t const buffer_mask_;
    char padding1[CACHE_LINE_SIZE - sizeof(size_t)];
};

#endif //DYNA_WFSPSCQ_H
