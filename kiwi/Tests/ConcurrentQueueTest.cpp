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

int ConcurrentQueueTest::numOfThreads = 8;

TEST_F(ConcurrentQueueTest, TestConcurrentPushSynchedPop) {
    const auto num_of_pushes = (1024 * 64) + 1;
    const auto min_val = 1337;
    auto total_inserted = 0;

    std::vector<std::thread> threads;
    for (auto i = 0; i < numOfThreads; i++) {
        const auto to_insert = num_of_pushes / numOfThreads;
        threads.push_back(spawnPushingThread(to_insert, min_val * i));
        total_inserted += to_insert;
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto pop_count = 0;
    std::vector<int> all_popped_vector(total_inserted);
    std::set<int> all_popped_set;
    while (pop_count < total_inserted) {
        int popped = -1;
        if(getQueue().try_pop(popped)) {
            std::cout << "Thread " << getThreadId() <<  " popped: " << popped << "\n";
            all_popped_set.insert(popped);
            all_popped_vector[pop_count] = popped;
            pop_count++;
            EXPECT_EQ(all_popped_set.size(), all_popped_vector.size());
        }
    }
    EXPECT_EQ(pop_count, total_inserted);
}

