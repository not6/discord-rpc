#pragma once

#include <atomic>

// A simple queue. No locks, but only works with a single thread as producer and a single thread as
// a consumer. Mutex up as needed.

template <typename ElementType, size_t QueueSize>
class MsgQueue {
private:
    ElementType queue_[QueueSize];
    std::atomic_uint nextAdd_{0};
    std::atomic_uint nextSend_{0};
    std::atomic_uint pendingSends_{0};

public:
    MsgQueue() noexcept = default;

    void Reset() noexcept
    {
        nextAdd_.store(0);
        nextSend_.store(0);
        pendingSends_.store(0);
    }

    bool HavePendingSends() const noexcept { return pendingSends_.load() != 0; }
    void CommitAdd() noexcept              { ++pendingSends_; }
    void CommitSend() noexcept             { --pendingSends_; }

    ElementType* GetNextAddMessage() noexcept
    {
        // if we are falling behind, bail
        if (pendingSends_.load() >= QueueSize) {
            return nullptr;
        }
        auto index = (nextAdd_++) % QueueSize;
        return &queue_[index];
    }

    ElementType* GetNextSendMessage() noexcept
    {
        auto index = (nextSend_++) % QueueSize;
        return &queue_[index];
    }
};
