#ifndef __KIWI_KIWIPQMOCK_H__
#define __KIWI_KIWIPQMOCK_H__

#include "../kiwiqueue/Kiwi.inl"
#ifndef __linux__
#include "../lib/mingw-threading/thread.h"
#else
#include <thread>
#endif

#define KIWI_TEST_CHUNK_SIZE 4u

template <class Comparer, class Allocator, typename K, uint32_t N=KIWI_TEST_CHUNK_SIZE>
class KiwiPQMock : public KiWiPQ<Comparer, Allocator, K, N> {
    using chunk_t = KiwiChunk<Comparer, K, N>;
    using KiwiPQ = KiWiPQ<Comparer, Allocator, K, N>;

   public:
    KiwiPQMock(const K& begin_key,
               const K& end_key)
        : KiWiPQ<Comparer, Allocator, K, N>(begin_key, end_key),
          num_of_rebalances(0), should_use_policy(false) {}

    unsigned int getRebalanceCount() { return num_of_rebalances; }

    unsigned int getChunkPolicyThreshold() { return KIWI_TEST_CHUNK_SIZE * (3.0 / 4.0);}

    void setShouldUsePolicy(bool should) { should_use_policy = should; }

    /**
     * Counts the number of chunks in the queue
     * @note: Assumed to be run in a sequential manner
     * @return The number of chunks in the queue
     */
    unsigned int getNumOfChunks() {
        chunk_t* chunk = unset_mark(this->begin_sentinel.next);
        int chunkCount = 0;
        while (chunk != &this->end_sentinel) {
            chunkCount++;
            chunk = unset_mark(chunk->next);
        }
        return chunkCount;
    }

   protected:
    virtual void rebalance(chunk_t* chunk) {
        ATOMIC_FETCH_AND_INC_FULL(&num_of_rebalances);
        KiWiPQ<Comparer, Allocator, K, N>::rebalance(chunk);
    }

    virtual bool policy(volatile chunk_t* chunk) {
        return should_use_policy &&  (chunk->i >= getChunkPolicyThreshold());
    }

   private:
    volatile bool should_use_policy;
    volatile unsigned int num_of_rebalances;
};

#endif  // __KIWI_KIWIPQMOCK_H__
