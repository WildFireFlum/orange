//
// Created by Ynon on 10/04/2018.
//

#ifndef KIWI_QUEUETEST_H
#define KIWI_QUEUETEST_H

#include <gtest/gtest.h>
#include <memory>
#include "KiwiPqMock.h"
#include "../kiwiqueue/MockAllocator.h"

extern MockAllocator s_allocator;

template <typename T>
class MockComparer {
public:
    bool operator()(const T& t1, const T& t2) const { return t1 < t2; }
};

using kiwipq_t = KiwiPQMock<MockComparer<int>, int, MockAllocator>;

class QueueTest : public testing::Test {
public:
    QueueTest();

    virtual void SetUp();

    virtual void TearDown();

    virtual ~QueueTest() = default;

protected:
    kiwipq_t& getQueue();
    std::unique_ptr<kiwipq_t> m_pq;
};

#endif //KIWI_QUEUETEST_H
