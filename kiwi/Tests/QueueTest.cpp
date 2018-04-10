//
// Created by Ynon on 26/03/2018.
//

#include <gtest/gtest.h>
#include "../Kiwi.inl"
#include "../MockAllocator.h"

class QueueTest : public testing::Test {

};

template <typename T>
class MockComparer {
public:
    bool operator()(const T& t1, const T& t2) const {
        return t1 < t2;
    }
};

TEST_F(QueueTest, ThisTestIsAwesome) {
    auto allocator = MockAllocator();
    KiWiPQ<MockComparer<int>, int, MockAllocator> pq(allocator);
    pq.push(1);
    ASSERT_EQ("", "");
}
