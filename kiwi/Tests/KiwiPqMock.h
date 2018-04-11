//
// Created by Ynon on 11/04/2018.
//

#ifndef KIWI_KIWIPQMOCK_H
#define KIWI_KIWIPQMOCK_H

#include "../kiwiqueue/Kiwi.inl"

template <typename Comparer, typename K, typename Allocator_t>
class KiwiPQMock : public KiWiPQ<Comparer, K, Allocator_t> {
    using chunk_t = KiwiChunk<Comparer, K>;

   public:
    KiwiPQMock(Allocator_t* alloc,
               const K& begin_key,
               const K& end_key,
               unsigned int num_threads)
        : KiWiPQ<Comparer, K, Allocator_t>(alloc,
                                           begin_key,
                                           end_key,
                                           num_threads),
          num_of_rebalances(0) {}

    unsigned int getRebalanceCount() {
        return num_of_rebalances;
    }

   protected:
    virtual void rebalance(chunk_t* chunk) {
        ATOMIC_FETCH_AND_INC_FULL(&num_of_rebalances);
        KiWiPQ<Comparer, K, Allocator_t>::rebalance(chunk);
    }

   private:
    volatile unsigned int num_of_rebalances;
};

#endif  // KIWI_KIWIPQMOCK_H
