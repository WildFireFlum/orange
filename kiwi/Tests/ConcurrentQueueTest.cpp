//
// Created by Ynon on 26/03/2018.
//

#include "QueueTest.h"

#ifndef __linux__
#include "../lib/mingw-threading/thread.h"
#else
#include <thread>
#endif

class ConcurrentQueueTest : public QueueTest {
   public:
    ConcurrentQueueTest() = default;

    static int numOfThreads;

    virtual void SetUp() {
        m_allocator = new MockAllocator();
        m_pq = new kiwipq_t(m_allocator, 0, 13371337, numOfThreads);
    }

protected:
    std::thread spawnPushingThread(unsigned int numOfInsertions, int min_val) {
        auto result = std::thread([this, numOfInsertions, min_val](){
            for (unsigned int i = min_val; i < numOfInsertions; i++) {
                std::cout << "thread " << getThreadId() << " pushing " << i << "\n";
                m_pq->push(i);
            }
        });
        return std::move(result);
    }

    std::thread spawnPoppingThread(unsigned int numOfPops) {
        auto result = std::thread([this, numOfPops](){
            for (unsigned int i = 0; i < numOfPops; i++) {
                std::cout << "thread " << getThreadId() << " popping ";
                int popped = -1;
                if (m_pq->try_pop(popped)) {
                    std::cout << "value " << popped << "\n";
                    EXPECT_NE(popped, -1);
                    popped = -1;
                }
                else {
                    std::cout << "nothing, pop failed\n";
                }
            }
        });
        return std::move(result);
    }

};

int ConcurrentQueueTest::numOfThreads = 3;

TEST_F(ConcurrentQueueTest, TestMorePushThanPopOneChunk) {
    const int num_of_pushes = (1024 * 5) + 1;
    const int num_of_pops = 1024;
    const int min_val = 1337;
    std::thread t1 = spawnPushingThread(num_of_pushes / 2, 1337);
    std::thread t2 = spawnPoppingThread(num_of_pops);
    std::thread t3 = spawnPushingThread(num_of_pushes / 2, min_val * min_val);
    t1.join();
    t2.join();
    t3.join();
    int popped = -1;
    while (!getQueue().try_pop(popped)) {}
    EXPECT_EQ(popped, min_val);
}

