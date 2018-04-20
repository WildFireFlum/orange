#ifndef __KIWI_QUEUETEST_H__
#define __KIWI_QUEUETEST_H__

#include <gtest/gtest.h>
#include <memory>
#include "KiwiPqMock.h"
#include "../kiwiqueue/MockAllocator.h"

template <typename T>
class MockComparer {
public:
    bool operator()(const T& t1, const T& t2) const { return t1 > t2; }
};

using kiwipq_t = KiWiPQMock<MockComparer<int>, MockAllocator<>, int>;

class QueueTest : public testing::Test {
public:
    QueueTest();

    virtual void TearDown();

    void checkQueueSizeAndValidity(unsigned int size);

    virtual ~QueueTest() = default;

protected:
    kiwipq_t& getQueue();
    std::unique_ptr<kiwipq_t> m_pq;
};

#endif //__KIWI_QUEUETEST_H__
