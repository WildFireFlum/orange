#include "QueueTest.h"


QueueTest::QueueTest() : m_pq(nullptr) {}

void QueueTest::TearDown() {
    m_pq = nullptr;
    nextID = 0;
}

kiwipq_t& QueueTest::getQueue() { return *m_pq.get(); }

void QueueTest::checkQueueSizeAndValidity(unsigned int size) {
    // Make sure queue is sorted
    auto prev = -1;
    for (auto i = 0; i < size; i++) {
        int curr;
        EXPECT_TRUE(getQueue().try_pop(curr));
        // Every item is inserted only once
        EXPECT_GT(curr, prev);
        prev = curr;
    }
    // validate the queue is empty
    EXPECT_FALSE(getQueue().try_pop(prev));
}
