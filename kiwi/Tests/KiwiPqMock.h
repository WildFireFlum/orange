//
// Created by Ynon on 11/04/2018.
//

#ifndef KIWI_KIWIPQMOCK_H
#define KIWI_KIWIPQMOCK_H

#include "../kiwiqueue/Kiwi.inl"
#ifndef __linux__
#include "../lib/mingw-threading/thread.h"
#else
#include <thread>
#endif

template <typename Comparer, typename K, typename Allocator_t>
class KiwiPQMock : public KiWiPQ<Comparer, K, Allocator_t> {
    using chunk_t = KiwiChunk<Comparer, K>;
    using KiwiPQ = KiWiPQ<Comparer, K, Allocator_t>;

   public:
    KiwiPQMock(Allocator_t* alloc,
               const K& begin_key,
               const K& end_key,
               unsigned int num_threads)
        : KiWiPQ<Comparer, K, Allocator_t>(alloc,
                                           begin_key,
                                           end_key,
                                           num_threads),
          num_of_rebalances(0), should_use_policy(false) {}

    unsigned int getRebalanceCount() { return num_of_rebalances; }

    unsigned int getChunkPolicyThreshold() { return KIWI_CHUNK_SIZE * (3.0 / 4.0);}

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
        KiWiPQ<Comparer, K, Allocator_t>::rebalance(chunk);
    }

    virtual bool policy(volatile chunk_t* chunk) {
        return should_use_policy &&  (chunk->i >= getChunkPolicyThreshold());
    }

   private:
    volatile bool should_use_policy;
    volatile unsigned int num_of_rebalances;
};

#endif  // KIWI_KIWIPQMOCK_H
