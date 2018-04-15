#ifndef __GALOIS_KIWI_H__
#define __GALOIS_KIWI_H__

#include <algorithm>
#include <iostream>

#include "Allocator.h"
#include "Utils.h"

#define KIW_DEFAULT_CHUNK_SIZE  1024
#define ATOMIC_CAS_MB(p, o, n) __sync_bool_compare_and_swap(p, o, n)
#define ATOMIC_FETCH_AND_INC_FULL(p) __sync_fetch_and_add(p, 1)

template <class Comparer, typename K, uint32_t N>
class KiwiChunk;

template <class Comparer, typename K, uint32_t N>
class KiWiRebalancedObject;

enum ChunkStatus {
    INFANT_CHUNK = 0,
    NORMAL_CHUNK = 1,
    FROZEN_CHUNK = 2,
};

enum PPA_MASK {
    IDLE = (1 << 29) - 1,
    POP = 1 << 29,
    PUSH = 1 << 30,
    FROZEN = 1 << 31,
};

template <typename T>
inline bool is_marked(T* j) {
    return ((uintptr_t)j & (uintptr_t)0x01) != 0;
}

template <typename T>
inline T* unset_mark(T* j) {
    return reinterpret_cast<T*>((uintptr_t)j & ~(uintptr_t)0x01);
}

template <typename T>
inline T* set_mark(T* j) {
    return reinterpret_cast<T*>((uintptr_t)j | (uintptr_t)0x01);
}

/**
 * The f
 * @tparam Comparer - Compares keys
 * @tparam K
 */
template <class Comparer, typename K, uint32_t N>
class KiWiRebalancedObject {
   public:  // dummy field which is used by the heap when the node is freed.
    // (without it, freeing a node would corrupt a field, possibly affecting
    // a concurrent traversal.)
    void* dummy;

    KiwiChunk<Comparer, K, N>* volatile first; // the first chunk share this object
    KiwiChunk<Comparer, K, N>* volatile next;  // next potential chunk - nullptr when the engagement stage is over

    void init(KiwiChunk<Comparer, K, N>* f, KiwiChunk<Comparer, K, N>* n) {
        first = f;
        next = n;
    }
};

/**
 * Pre-allocated memory containing a sorted concurrent list
 * of nodes in which the priorities and the values are stored
 * @tparam Comparer - Compares keys
 * @tparam K        - Keys type
 * @tparam N        - Number of keys in a chunk
 */
template <class Comparer, typename K, uint32_t N=KIW_DEFAULT_CHUNK_SIZE>
class KiwiChunk {
    using rebalance_object_t = KiWiRebalancedObject<Comparer, K, N>;

   public:
    // dummy field which is used by the heap when the node is freed.
    // (without it, freeing a node would corrupt a field, possibly affecting
    // a concurrent traversal.)
    void* dummy;

    volatile uint32_t i;

    typedef struct element_s {
        K key;
        volatile int deleted;
        struct element_s* volatile next;
    } element_t;

    /// Two fixed sentinels keep the invariant of an element always
    /// having both a previous and a next element
    element_t begin_sentinel;
    element_t k[N];
    element_t end_sentinel;

    /// The minimal key in the list, except for the first non-sentinel chunk
    K min_key;

    /// A link to the next chunk
    KiwiChunk<Comparer, K, N>* volatile next;

    /// The status of the chunk
    volatile uint32_t status;

    /// The parent chunk during rebalacing (while this chunk is still INFANT)
    KiwiChunk<Comparer, K, N>* volatile parent;

    /// Ponits to the rebalanced object that in win in the consensus at the
    /// begging of rebalanced
    rebalance_object_t* volatile ro;

    /// An array of indices to push or pop from the chunk, its size is equal to
    /// the number of threads in the system (see new_chunk() in KiWiPQ)
    uint32_t ppa_len;
    uint32_t volatile ppa[0];

    void init(unsigned int num_threads) {
        // Used for debugging
        element_t* const UNINITIALIZED =
            reinterpret_cast<element_t* const>(0xdeadf00d & (~1)) ;
        begin_sentinel.next = unset_mark(&end_sentinel);
        // The next of end_sentinel must not have its LSB on, otherwise find is not correct
        end_sentinel.next = reinterpret_cast<element_t* const>(0xedde0000);
        status = INFANT_CHUNK;
        ppa_len = num_threads;
        this->i = 0;

        // initialize ppa entries
        for (int j = 0; j < ppa_len; j++) {
            ppa[j] = IDLE;
        }

        // initialize the elements struct
        for (int j = 0; j < N; j++) {
            k[j].next = UNINITIALIZED;
            k[j].deleted = false;
        }
    }

    /// Based on lock free list from "The art of multiprocessor programming"
    void find(const Comparer& compare,
              const K& key,
              element_t*& out_prev,
              element_t*& out_next) {
        element_t* pred = nullptr;
        element_t* curr = nullptr;
        element_t* succ = nullptr;

    retry:
        while (true) {
            pred = &begin_sentinel;
            curr = unset_mark(pred->next);

            while (true) {
                succ = curr->next;
                while (is_marked(curr->next)) {
                    if (!ATOMIC_CAS_MB(&(pred->next), unset_mark(curr),
                                       unset_mark(succ))) {
                        goto retry;
                    }
                    curr = unset_mark(succ);
                    succ = curr->next;
                }

                if (curr == &end_sentinel || !compare(curr->key, key)) {
                    out_prev = pred;
                    out_next = curr;
                    return;
                }

                if (unset_mark(succ) == &end_sentinel) {
                    out_prev = curr;
                    out_next = &end_sentinel;
                    return;
                }

                pred = curr;
                curr = unset_mark(succ);
            }
        }
    }

    void push(const Comparer& compare, element_t& element) {
        const K& key = element.key;
        while (true) {
            element_t* left;
            element_t* right;
            find(compare, key, left, right);

            element.next = right;
            if (ATOMIC_CAS_MB(&(left->next), unset_mark(right),
                              unset_mark(&element))) {
                return;
            }
        }
    }

    void freeze() {
        status = ChunkStatus::FROZEN_CHUNK;
        for (uint32_t j = 0; j < ppa_len; j++) {
            uint32_t ppa_j;
            do {
                ppa_j = ppa[j];
            } while (!(ppa_j & FROZEN) &&
                     !ATOMIC_CAS_MB(&ppa[j], ppa_j, ppa_j | FROZEN));
        }
    }

    bool publish_push(uint32_t index) {
        uint32_t thread_id = getThreadId();
        uint32_t ppa_t = ppa[thread_id];
        if (!(ppa_t & FROZEN)) {
            return ATOMIC_CAS_MB(&ppa[thread_id], ppa_t, PUSH | index);
        }
        return false;
    }

    bool publish_pop(uint32_t index) {
        uint32_t thread_id = getThreadId();
        uint32_t ppa_t = ppa[thread_id];
        if (!(ppa_t & FROZEN)) {
            return ATOMIC_CAS_MB(&ppa[thread_id], ppa_t, POP | index);
        }
        return false;
    }

    bool unpublish_index() {
        uint32_t thread_id = getThreadId();
        uint32_t ppa_t = ppa[thread_id];
        if (!(ppa_t & FROZEN)) {
            return ATOMIC_CAS_MB(&ppa[thread_id], ppa_t, IDLE);
        }
        return false;
    }

    // TODO: should be improved
    uint32_t get_keys_to_preserve_from_chunk(K (&arr)[N]) {
        if (status != FROZEN_CHUNK) {
            // invalid call
            return 0;
        }

        bool flags[N] = {false};

        // add all list elements
        element_t* element = begin_sentinel.next;
        while (element != &end_sentinel) {
            if (!element->deleted) {
                flags[element - k] = true;
            }
            element = unset_mark(element->next);
        }

        // add pending push
        for (int j = 0; j < ppa_len; j++) {
            uint32_t ppa_j = ppa[j];
            if (ppa_j & PUSH) {
                uint32_t index = ppa_j & IDLE;
                if (index < N) {
                    flags[index] = true;
                }
            }
        }

        // remove pending pop
        for (int j = 0; j < ppa_len; j++) {
            uint32_t ppa_j = ppa[j];
            if (ppa_j & POP) {
                uint32_t index = ppa_j & IDLE;
                if (index < N) {
                    flags[index] = false;
                }
            }
        }

        uint32_t count = 0;
        for (int j = 0; j < N; j++) {
            if (flags[j]) {
                arr[count++] = k[j].key;
            }
        }
        return count;
    }

    bool try_pop(const Comparer& comparer, K& key) {
        if (status == FROZEN_CHUNK) {
            return false;
        }

        element_t* currElem = &begin_sentinel;

        while (true) {
            // 1. find not deleted element
            do {
                currElem = unset_mark(currElem->next);
            } while ((currElem != &end_sentinel) && (currElem->deleted));

            if (currElem == &end_sentinel) {
                // end of the list
                return false;
            }

            // 2. publish pop
            if (!publish_pop((uint32_t)(currElem - k))) {
                // chunk is being rebalanced
                return false;
            }

            // 3. try to mark element as deleted
            if (!ATOMIC_CAS_MB(&(currElem->deleted), 0, 1)) {
                // someone else deleted the element before us, continue
                continue;
            }

            // 4. deleted - pop from elements list
            if (currElem == &end_sentinel) {
                return false;
            }

            key = currElem->key;
            element_t* nextElem;
            do {
                nextElem = currElem->next;
                if (is_marked(nextElem)) {
                    break;
                }
            } while (
                !ATOMIC_CAS_MB(&currElem->next, nextElem, set_mark(nextElem)));

            element_t *prev = nullptr, *next = nullptr;
            find(comparer, key, prev, next);
            return true;
        }
    }

    /// Prints the queue and counts its elements, used for debugging and is not
    /// synchronized,
    /// TODO: remove
    unsigned int printAndCount() {
        element_t* e = unset_mark(begin_sentinel.next);
        unsigned int chunkCount = 0;
        while (e != &end_sentinel) {
            std::cout << reinterpret_cast<int>(e->key) << " -> ";
            e = unset_mark(e->next);
            chunkCount++;
        }

        std::cout << "\\\n count = " << chunkCount << "\n";
        return chunkCount;
    }

    /**
     * Counts the number of elements in a chunk, not synchronized
     * @note: Assumed to be run in a sequential manner
     * @return The number of elements in a chunk
     */
    unsigned int size() {
        element_t* e = unset_mark(begin_sentinel.next);
        unsigned int chunkCount = 0;
        while (e != &end_sentinel) {
            e = unset_mark(e->next);
            chunkCount++;
        }
        return chunkCount;
    }
};

#ifdef GALOIS
#include "GaloisAllocator.h"
#endif

template <typename Comparer, typename K, typename Allocator_t, uint32_t N>
class KiWiPQ {
    using chunk_t = KiwiChunk<Comparer, K, N>;
    using rebalance_object_t = KiWiRebalancedObject<Comparer, K, N>;

   protected:
    // keys comparator
    Comparer compare;

    Allocator_t* allocator;

    /// The number of threads
    unsigned int num_of_threads;

    // chunks
    chunk_t begin_sentinel;
    chunk_t end_sentinel;
    // LockFreeSkipListSet<Comparer, K, chunk_t*> index;

    chunk_t* new_chunk() {
        // Second argument is an index of a freelist to use to reclaim
        chunk_t* chunk = reinterpret_cast<chunk_t*>(allocator->allocate(
            sizeof(chunk_t) + sizeof(uint32_t) * num_of_threads, 0));
        chunk->init(num_of_threads);
        return chunk;
    }

    inline void reclaim_chunk(chunk_t* chunk) { allocator->reclaim(chunk, 0); }

    inline void delete_chunk(chunk_t* chunk) { allocator->deallocate(chunk, 0); }

    rebalance_object_t* new_ro(chunk_t* f, chunk_t* n) {
        rebalance_object_t* ro = reinterpret_cast<rebalance_object_t*>(
            allocator->allocate(sizeof(rebalance_object_t), 1));
        ro->init(f, n);
        return ro;
    }

    void reclaim_ro(rebalance_object_t* ro) { allocator->reclaim(ro, 1); }

    void delete_ro(rebalance_object_t* ro) { allocator->deallocate(ro, 1); }

    bool check_rebalance(chunk_t* chunk, const K& key) {
        if (chunk->status == INFANT_CHUNK) {
            // TODO: it is clear why they think it is enough to normalize at
            // that point, but we don't have the required information (Cn, Cf,
            // last are all nullptr...) normalize(chunk->parent);
            ATOMIC_CAS_MB(&(chunk->status), INFANT_CHUNK, NORMAL_CHUNK);
            return true;
        }
        if (chunk->i >= N || chunk->status == FROZEN_CHUNK ||
            policy(chunk)) {
            rebalance(chunk);
            return true;
        }
        return false;
    }

    virtual void rebalance(chunk_t* chunk) {
        // 1. engage
        rebalance_object_t* tmp = new_ro(chunk, unset_mark(chunk->next));
        if (!ATOMIC_CAS_MB(&(chunk->ro), nullptr, tmp)) {
            reclaim_ro(tmp);
        }
        rebalance_object_t* ro = chunk->ro;

        volatile chunk_t* last = chunk;
        while (true) {
            chunk_t* next = unset_mark(ro->next);
            if (next == nullptr || next == &end_sentinel) {
                break;
            }
            if (policy(next)) {
                ATOMIC_CAS_MB(&(next->ro), nullptr, ro);

                if (next->ro == ro) {
                    ATOMIC_CAS_MB(&(ro->next), next, unset_mark(next->next));
                    last = next;
                } else {
                    ATOMIC_CAS_MB(&(ro->next), next, nullptr);
                }
            } else {
                ATOMIC_CAS_MB(&(ro->next), next, nullptr);
            }
        }

        // search for last concurrently engaged chunk
        while (unset_mark(last->next)->ro == ro) {
            last = unset_mark(last->next);
        }

        // 2. freeze
        chunk_t* t = ro->first;
        do {
            t->freeze();
        } while ((t != last) && (t = unset_mark(t->next)));

        // 3. pick minimal version
        // ... we don't have scans so we don't need this part

        // 4. build:
        chunk_t* c = ro->first;
        chunk_t* Cn = new_chunk();
        chunk_t* Cf = Cn;

        do {
            K arr[N];
            uint32_t count = c->get_keys_to_preserve_from_chunk(arr);
            std::sort(arr, arr + count, compare);
            for (uint32_t j = 0; j < count; j++) {
                if (Cn->i > (N / 2)) {
                    // Cn is more than half full - create new chunk

                    Cn->min_key = Cn->k[0].key;  // set Cn min key - this value
                                                 // won't be change
                    Cn->begin_sentinel.next =
                        &Cn->k[0];  // connect begin sentinel to the list
                    Cn->k[Cn->i - 1].next = &(
                        Cn->end_sentinel);   // close the list by point the last
                                             // key to the end sentinel
                    Cn->next = new_chunk();  // create a new chunk and set
                                             // Cn->next points to it
                    Cn = Cn->next;           // Cn points to the new chunk

                    Cn->parent = ro->first;  // set chunk as rebalance parent of the
                                             // new chunk

                    // TODO: delete it as soon as we use index again
                    Cn->status = NORMAL_CHUNK;
                }
                volatile uint32_t& i = Cn->i;
                Cn->k[i].key = arr[j];
                Cn->k[i].next = &(Cn->k[i + 1]);
                i++;
            }
        } while ((c != last) && (c = unset_mark(c->next)));

        bool is_empty = false;

        if (Cn->i > 0) {
            // we need to close the last chunk as well
            Cn->min_key =
                Cn->k[0].key;  // set Cn min key - this value won't be change
            Cn->begin_sentinel.next =
                &Cn->k[0];  // connect begin sentinel to the list
            Cn->k[Cn->i - 1].next = &(Cn->end_sentinel);  // close the list by
                                                          // point the last key
                                                          // to the end sentinel
        } else {
            // all the chunks in ro are empty
            reclaim_chunk(Cn);
            Cf = Cn = nullptr;
            is_empty = true;
        }

        // 5. replace
        do {
            c = last->next;
        } while (!is_marked(c) &&
                 !ATOMIC_CAS_MB(&(last->next), unset_mark(c), set_mark(c)));

        if (!is_empty) {
            Cn->next = unset_mark(c);
        }

        do {
            // TODO: should validate this part ...
            chunk_t* pred = load_prev(ro->first);

            if (pred == nullptr) {
                // ro-> first is not accessible - someone else succeeded - delete the chunks we just created and return
                if (!is_empty) {
                    chunk_t* curr = Cf;
                    chunk_t* next;
                    do {
                        next = unset_mark(curr->next);
                        reclaim_chunk(curr);
                    } while ((curr != Cn) && (curr = next));
                }

                // we can help normalize here if we want
                return;
            }

            if (!is_empty) {
                c = Cf;
            }

            if (ATOMIC_CAS_MB(&(pred->next), unset_mark(ro->first), unset_mark(c))) {
                // success - normalize chunk and free old chunks and normalize
                // normalize
                chunk_t* curr = ro->first;
                chunk_t* next;
                do {
                    next = unset_mark(curr->next);
                    reclaim_chunk(curr);
                } while ((curr != last) && (curr = next));

                reclaim_ro(ro);
                return;
            }

            if (pred->status == FROZEN_CHUNK && unset_mark(pred->next) == ro->first) {
                // the predecessor is being rebalanced - help it and retry
                rebalance(pred);
            }

        } while (true);
    }

    chunk_t* locate_target_chunk(const K& key) {
        chunk_t* c = &begin_sentinel;  // index.get(key);
        chunk_t* next = unset_mark(c->next);
        if (next == &end_sentinel) {
            // the chunk list is empty, we need to create a chunk
            chunk_t* chunk = new_chunk();
            chunk->min_key = key;  // set chunk's min key
            chunk->next =
                &end_sentinel;  // set chunk->next point to end sentinel
            // try to connect the new chunk to the list
            if (!ATOMIC_CAS_MB(&(begin_sentinel.next),
                               unset_mark(&end_sentinel), unset_mark(chunk))) {
                // add failed - delete chunk.
                reclaim_chunk(chunk);
            }
            return locate_target_chunk(key);
        }

        while (next != &end_sentinel && !compare(key, next->min_key)) {
            c = next;
            next = unset_mark(c->next);
        }

        if (c == &begin_sentinel) {
            // we never add any key to the sentinels
            return begin_sentinel.next;
        }

        return c;
    }

    chunk_t* load_prev(chunk_t* chunk) {
        // TODO: should use index instead of traversing the list
        chunk_t* prev = &begin_sentinel;
        chunk_t* curr = unset_mark(prev->next);

        while (curr != chunk && curr != &end_sentinel && !compare(chunk->min_key, curr->min_key)) {
            prev = curr;
            curr = unset_mark(prev->next);
        }

        if (curr != chunk) {
            return nullptr;
        }

        return prev;
    }

    void normalize(chunk_t* chunk) {
        // TODO
    }

    bool policy(volatile chunk_t* chunk) {
        // TODO ....
        return false;  // chunk->i > (N * 3 / 4) || chunk->i <
        // (N / 4);
    }

   public:
#ifdef GALOIS
    KiWiPQ()
        : allocator(new GaloisAllocator()),
          begin_sentinel(),
          end_sentinel(),
          num_of_threads(getNumOfThreads()) {
        begin_sentinel.next = &end_sentinel;
    }
#endif

    KiWiPQ(Allocator_t* alloc,
           const K& begin_key,
           const K& end_key,
           unsigned int num_threads)
        : allocator(alloc),
          begin_sentinel(),
          end_sentinel(),
          num_of_threads(num_threads) {
        begin_sentinel.next = &end_sentinel;
        begin_sentinel.min_key = begin_key;
        end_sentinel.min_key = end_key;
    }

    virtual ~KiWiPQ() = default;

    bool push(const K& key) {
        chunk_t* chunk = locate_target_chunk(key);

        if (check_rebalance(chunk, key)) {
            return push(key);
        }

        // allocate cell in linked list
        uint32_t i = ATOMIC_FETCH_AND_INC_FULL(&chunk->i);

        if (i >= N) {
            // no more free space - trigger rebalance
            rebalance(chunk);
            return push(key);
        }

        chunk->k[i].key = key;

        if (!chunk->publish_push(i)) {
            // chunk is being rebalanced
            rebalance(chunk);
            return push(key);
        }

        chunk->push(compare, chunk->k[i]);
        chunk->unpublish_index();
        return true;
    }

    bool try_pop(K& key) {
        chunk_t* chunk = unset_mark(begin_sentinel.next);
        while (chunk != &end_sentinel) {
            if (chunk->try_pop(compare, key)) {
                return true;
            }

            if (chunk->status == FROZEN) {
                // chunk is being rebalanced
                rebalance(chunk);
                return try_pop(key);
            }

            chunk = unset_mark(chunk->next);
        }
        return false;
    }

    /// Prints the queue and counts its elements, used for debugging and is not
    /// synchronized
    /// TODO: remove
    unsigned int printAndCount() {
        std::cout << "\n pq state:\n";
        chunk_t* chunk = unset_mark(begin_sentinel.next);
        int inChunkCount = 0;
        unsigned int totalCount = 0;
        while (chunk != &end_sentinel) {
            std::cout << "(" << inChunkCount++ << " - " << chunk << " ) ";
            totalCount += chunk->printAndCount();
            chunk = unset_mark(chunk->next);
        }
        std::cout << "total count " << totalCount << "\n";
        std::cout << "\n-------------\n\n";

        return totalCount;
    }

    /**
     * Counts the number of elements in the queue
     * @note: Assumed to be run in a sequential manner
     * @return The number of elements in a chunk
     */
    unsigned int size() {
        chunk_t* chunk = unset_mark(begin_sentinel.next);
        int inChunkCount = 0;
        unsigned int totalCount = 0;
        while (chunk != &end_sentinel) {
            totalCount += chunk->size();
            chunk = unset_mark(chunk->next);
        }

        return totalCount;
    }
};

#endif  // __GALOIS_KIWI_H__