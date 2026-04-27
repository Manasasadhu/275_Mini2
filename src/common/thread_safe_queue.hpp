#ifndef THREAD_SAFE_QUEUE_HPP
#define THREAD_SAFE_QUEUE_HPP

#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>

// Bounded, thread-safe queue with backpressure.
// Producers block when full; consumers block when empty.
template<typename T>
class ThreadSafeQueue {
public:
    explicit ThreadSafeQueue(size_t capacity = 32) : capacity_(capacity) {}

    // Move-only (mutex is not copyable)
    ThreadSafeQueue(ThreadSafeQueue&&) = delete;
    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

    // Blocking push — returns false if queue was finished while waiting
    bool push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_cv_.wait(lock, [this]{ return finished_ || queue_.size() < capacity_; });
        if (finished_) return false;
        queue_.push(std::move(item));
        not_empty_cv_.notify_one();
        return true;
    }

    // Timed pop — returns true if an item was retrieved
    template<typename Rep, typename Period>
    bool wait_pop_for(T& item, const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!not_empty_cv_.wait_for(lock, timeout,
                [this]{ return finished_ || !queue_.empty(); })) {
            return false;
        }
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        not_full_cv_.notify_one();
        return true;
    }

    // Signal no more items will be pushed
    void set_finished() {
        std::lock_guard<std::mutex> lock(mutex_);
        finished_ = true;
        not_empty_cv_.notify_all();
        not_full_cv_.notify_all();
    }

    // True when finished AND drained
    bool is_finished() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return finished_ && queue_.empty();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

private:
    mutable std::mutex      mutex_;
    std::queue<T>           queue_;
    std::condition_variable not_empty_cv_;
    std::condition_variable not_full_cv_;
    bool   finished_  = false;
    size_t capacity_;
};

#endif // THREAD_SAFE_QUEUE_HPP