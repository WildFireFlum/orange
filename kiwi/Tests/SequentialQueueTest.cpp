#include "QueueTest.h"
#include <algorithm>

class SequentialQueueTest : public QueueTest {
   public:
    SequentialQueueTest() {
        numberOfThreads = 1;
    }

    virtual void SetUp() {
        QueueTest::SetUp();
        m_pq.reset(new kiwipq_t(-13371337, 13371337));
    }
};

TEST_F(SequentialQueueTest, TestOnePushOnePop) {
    const int num_to_push = 1;
    int num_to_pop = -1;
    auto& pq = getQueue();

    pq.push(num_to_push);
    pq.try_pop(num_to_pop);
    EXPECT_EQ(num_to_pop, num_to_push);
}

TEST_F(SequentialQueueTest, TestMultiPushAscendingOnePopOneChunk) {
    const int first_num_to_push = 10;
    auto& pq = getQueue();
    int num_to_pop = -1;

    for (int i = first_num_to_push; i < KIWI_TEST_CHUNK_SIZE - 10; i++) {
        pq.push(i);
    }

    pq.try_pop(num_to_pop);
    EXPECT_EQ(num_to_pop, first_num_to_push);
}

TEST_F(SequentialQueueTest, TestMultiPushDecendingOnePopOneChunk) {
    const int expected_pop = 10;
    auto& pq = getQueue();
    int num_to_pop = -1;

    for (int i = KIWI_TEST_CHUNK_SIZE - 10; i >= expected_pop; i--) {
        pq.push(i);
    }

    pq.try_pop(num_to_pop);
    EXPECT_EQ(num_to_pop, expected_pop);
}

TEST_F(SequentialQueueTest, TestMultiPushDecendingMultiPopOneChunk) {
    const int FIRST_POP = 10;
    const int LAST_POP = KIWI_TEST_CHUNK_SIZE - 10;
    auto& pq = getQueue();

    for (int i = LAST_POP; i >= FIRST_POP; i--) {
        pq.push(i);
    }

    for (int expected_pop = FIRST_POP; expected_pop <= LAST_POP;
         expected_pop++) {
        int num_to_pop = -1;
        pq.try_pop(num_to_pop);
        EXPECT_EQ(num_to_pop, expected_pop);
    }
}

TEST_F(SequentialQueueTest, TestMultiPushPopDecendingOneChunk) {
    const int FIRST_POP = 10;
    const int LAST_POP = KIWI_TEST_CHUNK_SIZE - 10;
    auto& pq = getQueue();

    for (int i = FIRST_POP; i >= LAST_POP; i--) {
        int popped = -1;
        pq.push(i);
        pq.try_pop(popped);
        EXPECT_EQ(popped, i);
    }
}

TEST_F(SequentialQueueTest, TestMultiPushPopAscendingOneChunk) {
    const int FIRST_PUSH = 10;
    const int LAST_PUSH = KIWI_TEST_CHUNK_SIZE - 10;
    auto& pq = getQueue();

    for (int i = FIRST_PUSH; i <= LAST_PUSH; i++) {
        int popped = -1;
        pq.push(i);
        pq.try_pop(popped);
        EXPECT_EQ(popped, i);
    }
}

TEST_F(SequentialQueueTest, TestMultiPushOnePopDecendingMultipleChunks) {
    // this test was correct as long as policy returned false constantly
    const int NUM_OF_CHUNKS = 19;
    const int FIRST_POP = 10;
    const int LAST_POP = (KIWI_TEST_CHUNK_SIZE * NUM_OF_CHUNKS) + 10;
    auto& pq = getQueue();

    for (int i = FIRST_POP; i <= LAST_POP; i++) {
        pq.push(i);
    }

    int popped = -1;
    pq.try_pop(popped);
    EXPECT_EQ(popped, FIRST_POP);
    EXPECT_EQ(getQueue().getRebalanceCount(), (NUM_OF_CHUNKS - 1) * 2);
}

TEST_F(SequentialQueueTest, TestHeapSort) {
    const int COUNT = (KIWI_TEST_CHUNK_SIZE) * 5 + 10;
    auto& pq = getQueue();

    srand(0xdeadbeef);

    int arr[COUNT];
    for (int i = 0 ; i < COUNT; i ++) {
        arr[i] = std::rand();
        EXPECT_TRUE(pq.push(arr[i]));
    }

    int popped = -1;
    std::sort(arr, arr + COUNT);
    for (int i = 0 ; i < COUNT; i ++) {
        EXPECT_TRUE(pq.try_pop(popped));
        EXPECT_EQ(arr[i], popped);
    }
}