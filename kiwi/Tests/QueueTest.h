//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_QUEUETEST_H
#define KIWI_QUEUETEST_H

#include <gtest/gtest.h>
#include "KiwiPqMock.h"
#include "../kiwiqueue/MockAllocator.h"

template <typename T>
class MockComparer {
public:
    bool operator()(const T& t1, const T& t2) const { return t1 < t2; }
};

using kiwipq_t = KiwiPQMock<MockComparer<int>, int, MockAllocator>;

class QueueTest : public testing::Test {
public:
    QueueTest() : m_allocator(nullptr), m_pq(nullptr) {}

    virtual void TearDown() {
        delete m_allocator;
        delete m_pq;
    }

protected:
    kiwipq_t& getQueue() { return *m_pq; }
    MockAllocator* m_allocator;
    kiwipq_t* m_pq;
};

#endif //KIWI_QUEUETEST_H
