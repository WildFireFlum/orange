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
        m_pq = new kiwipq_t(m_allocator, -13371337, 13371337, numOfThreads);
    }

   protected:
    std::thread spawnPushingThread(unsigned int numOfInsertions, int min_val) {
        auto result = std::thread([this, numOfInsertions, min_val]() {
            for (unsigned int i = min_val; i < numOfInsertions + min_val; i++) {
                std::cout << "thread " << getThreadId() << " pushing " << i
                          << "\n";
                m_pq->push(i);
            }
        });
        return std::move(result);
    }

    std::thread spawnPoppingThread(unsigned int numOfPops) {
        auto result = std::thread([this, numOfPops]() {
            // Try to make sure we don't pop first
            std::this_thread::yield();
            unsigned int popCount = 0;
            while (popCount < numOfPops) {
                std::cout << "thread " << getThreadId() << " popping ";
                int popped = -1;
                if (m_pq->try_pop(popped)) {
                    std::cout << "value " << popped << "\n";
                    EXPECT_NE(popped, -1);
                    popped = -1;
                    popCount++;
                } else {
                    std::cout << "nothing, pop failed\n";
                }
            }
        });
        return std::move(result);
    }
};

int ConcurrentQueueTest::numOfThreads = 8;

TEST_F(ConcurrentQueueTest, TestConcurrentPushSynchedPop) {
    const auto num_of_pushes = (KIWI_CHUNK_SIZE * 64) + 1;
    // Make sure no duplicate values
    const auto min_val = (num_of_pushes / numOfThreads) + 1;
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

    // Make sure that all items were pushed
    EXPECT_EQ(total_inserted, getQueue().size());
    int prev = -1;
    for (auto i = 0; i < total_inserted; i++) {
        int curr;
        EXPECT_TRUE(getQueue().try_pop(curr));
        // Every item is inserted only once
        EXPECT_GT(curr, prev);
        prev = curr;
    }
    // validate the queue is empty
    EXPECT_FALSE(getQueue().try_pop(prev));
}

TEST_F(ConcurrentQueueTest, TestConcurrentRebalances) {
    const int numToPush = 1;
    auto pushNumber = [this]() { getQueue().push(numToPush); };

    // Fill a chunk with ones
    for (int i = 0; i < KIWI_CHUNK_SIZE; i++) {
        getQueue().push(numToPush);
    }
    EXPECT_EQ(getQueue().getNumOfChunks(), 1);
    EXPECT_EQ(getQueue().getRebalanceCount(), 0);

    std::vector<std::thread> threads;
    for (auto i = 0; i < numOfThreads; i++) {
        threads.emplace_back(pushNumber);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Make sure that all items were pushed
    EXPECT_GT(getQueue().getRebalanceCount(), 1);
    EXPECT_EQ(getQueue().getNumOfChunks(), 2);
    std::cout << "Rebalance count: " << getQueue().getRebalanceCount() << "\n";
}

TEST_F(ConcurrentQueueTest, TestStressPushPop) {
    const auto num_of_pushes = KIWI_CHUNK_SIZE * 1024;
    const auto num_of_pops = KIWI_CHUNK_SIZE * 64;
    const auto num_of_popping_threads = numOfThreads * 0.50;
    const auto num_of_pushing_threads = numOfThreads * 0.50;

    // Make sure no duplicate values
    const auto min_val = (num_of_pushes / num_of_pushing_threads) + 1;

    std::vector<std::thread> threads;
    for (auto i = 0; i < num_of_pushing_threads; i++) {
        const auto to_insert = num_of_pushes / num_of_pushing_threads;
        threads.push_back(spawnPushingThread(to_insert, min_val * i));
    }

    for (auto i = 0; i < num_of_popping_threads; i++) {
        threads.push_back(spawnPoppingThread(num_of_pops / num_of_popping_threads));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    const auto queueFinalSize = getQueue().size();
    // Make sure that all items were pushed
    EXPECT_EQ(num_of_pushes - num_of_pops, queueFinalSize);


    // Make sure queue is sorted
    auto prev = -1;
    for (auto i = 0; i < queueFinalSize; i++) {
        int curr;
        EXPECT_TRUE(getQueue().try_pop(curr));
        // Every item is inserted only once
        EXPECT_GT(curr, prev);
        prev = curr;
    }
    // validate the queue is empty
    EXPECT_FALSE(getQueue().try_pop(prev));
}
