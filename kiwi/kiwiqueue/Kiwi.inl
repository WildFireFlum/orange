#ifndef __GALOIS_KIWI_H__
#define __GALOIS_KIWI_H__

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "Utils.h"
#include "Index.h"


template <class Comparer, typename K, uint32_t N>
class KiWiChunk;

template <class Comparer, typename K, uint32_t N>
class KiWiRebalancedObject;

enum ChunkStatus {
    UNINITIALIZED_CHUNK = 0,
    INFANT_CHUNK        = 1,
    NORMAL_CHUNK        = 2,
    FROZEN_CHUNK        = 3,
};

enum PPA_MASK {
    IDLE                = (1 << 29) - 1 ,
    POP                 = 1 << 29       ,
    PUSH                = 1 << 30       ,
    FROZEN              = 1 << 31       ,
};


/**
 * Since multiple threads may simultaneously execute rebalance(C), they need
 * to reach consensus regarding the set of engaged chunks. The consensus is
 * managed via pointers from the chunks to a dedicated rebalance object - ro.
 * Object of this class represent a single ro.
 *
 * @tparam Comparer - Compares keys
 * @tparam K        - Keys type
 * @tparam N        - Number of keys in a chunk
 */
template <class Comparer, typename K, uint32_t N>
class KiWiRebalancedObject {
   public:  // dummy field which is used by the heap when the node is freed.
    // (without it, freeing a node would corrupt a field, possibly affecting
    // a concurrent traversal.)
    void* dummy;

    KiWiChunk<Comparer, K, N>* volatile first;  // the first chunk share this object
    KiWiChunk<Comparer, K, N>* volatile next;   // next potential chunk - nullptr or &end_sentinel
                                                // when the engagement stage is over

    void init(KiWiChunk<Comparer, K, N>* f, KiWiChunk<Comparer, K, N>* n) {
        first = f;
        next = n;
    }
};

/**
 * KiWi data structure is organized as a collection of large blocks of
 * contiguous key ranges, called chunks. Object of this class represent
 * a single chunk in the data structure.
 *
 * @tparam Comparer - Compares keys
 * @tparam K        - Keys type
 * @tparam N        - Number of keys in a chunk
 */
template <class Comparer, typename K, uint32_t N>
class KiWiChunk {
    using rebalance_object_t = KiWiRebalancedObject<Comparer, K, N>;

   public:
    // dummy field which is used by the heap when the node is freed.
    // (without it, freeing a node would corrupt a field, possibly affecting
    // a concurrent traversal.)
    void* dummy;

    volatile uint32_t i;

    typedef struct element_s {
        K key;
        struct element_s* volatile next;
    } element_t;

    /// Two fixed sentinels keep the invariant of an element always
    /// having both a previous and a next element
    element_t begin_sentinel;
    element_t k[N];
    element_t end_sentinel;

    /// The minimal key in the list -
    /// Note that this field is immutable and doesn't change
    /// even after the minimal key was deleted
    K min_key;

    /// A link to the next chunk
    KiWiChunk<Comparer, K, N>* volatile next;

    /// The status of the chunk
    volatile uint32_t status;

    /// The parent of the chunk (equivalent to parent process)
    KiWiChunk<Comparer, K, N>* volatile parent;

    /// Ponits to the rebalanced object that win in the consensus at the
    /// begging of rebalanced (nullptr in initialization time)
    rebalance_object_t* volatile ro;

    /// An array of indices to push or pop from the chunk, its size is equal to
    /// the number of threads in the system (see new_chunk() in KiWiPQ)
    uint32_t ppa_len;
    uint32_t volatile ppa[0];

    void init(unsigned int num_threads) {
        // clean memory (not including the dummy and ppa array)
        memset((char*)this + sizeof(dummy), 0, sizeof(*this) - sizeof(dummy));

        // initialize sentinels
        begin_sentinel.next = &end_sentinel;

        // initialize ppa entries
        ppa_len = num_threads;
        for (int j = 0; j < ppa_len; j++) {
            ppa[j] = IDLE;
        }

        // set status infant
        status = INFANT_CHUNK;
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

                // physically remove node from the beginning of the list marked nodes
                while (is_marked(curr->next)) {
                    if (!ATOMIC_CAS_MB(&(pred->next), unset_mark(curr),
                                       unset_mark(succ))) {
                        goto retry;
                    }
                    curr = unset_mark(succ);
                    succ = curr->next;
                }

                // if we find the key (pred->key < key <= curr->key) or reached the end of the list than return
                if (curr == &end_sentinel || !compare(key, curr->key)) {
                    out_prev = pred;
                    out_next = curr;
                    return;
                }

                // if succ is the end of the list we can return as well with curr as prev
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
            if (!is_marked(right) &&
                ATOMIC_CAS_MB(&(left->next), unset_mark(right), unset_mark(&element))) {
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

    uint32_t get_keys_to_preserve_from_chunk(K (&arr)[N]) {
        if (status != FROZEN_CHUNK) {
            // invalid call
            return 0;
        }

        bool flags[N] = {false};

        // add all list elements
        element_t* element = begin_sentinel.next;
        while (element != &end_sentinel) {
            flags[element - k] = true;
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

    bool try_pop(const Comparer& compare, K& key) {
        if (status == FROZEN_CHUNK) {
            return false;
        }


    retry:

        element_t* pred = &begin_sentinel;
        element_t* curr = begin_sentinel.next;

        while (true) {
            // 1. find an element to pop - physically remove node from the
            //    beginning of the list marked nodes
            element_t* succ = curr->next;
            while (is_marked(succ)) {
                if (!ATOMIC_CAS_MB(&(pred->next), unset_mark(curr), unset_mark(succ))) {
                    goto retry;
                }
                curr = unset_mark(succ);
                succ = curr->next;
            }

            if (curr == &end_sentinel) {
                // end of the list
                return false;
            }

            // 2. publish pop - the index of curr is curr - k (pointer's arithmetic)
            if (!publish_pop((uint32_t)(curr - k))) {
                // chunk is being rebalanced
                return false;
            }

            // 3. try to mark element as deleted
            key = curr->key;
            element_t* nextElem;
            do {
                nextElem = curr->next;
                if (is_marked(nextElem)) {
                    // some one else deleted curr, look for other element to pop
                    goto retry;
                }
            } while (!ATOMIC_CAS_MB(&curr->next, nextElem, set_mark(nextElem)));

            return true;
        }
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
            if (!is_marked(e->next)) chunkCount++;
            e = unset_mark(e->next);
        }
        return chunkCount;
    }
};


template <class Comparer, class Allocator, typename K, uint32_t N = KIWI_DEFAULT_CHUNK_SIZE>
class KiWiPQ {
    using chunk_t = KiWiChunk<Comparer, K, N>;
    using rebalance_object_t = KiWiRebalancedObject<Comparer, K, N>;

protected:
    /// Keys comparator
    Comparer compare;

    /// Memory allocator - support memory reclamation
    Allocator allocator;

    /// Chunks list sentinels
    chunk_t begin_sentinel;
    chunk_t end_sentinel;

    /// Shortcut to reach a required chunk (instead of traversing the entire list)
    Index<Comparer, Allocator, K, chunk_t*> index;

    inline chunk_t* new_chunk(chunk_t* parent) {
        // Second argument is an index of a freelist to use to reclaim
        unsigned int num_of_threads = getNumOfThreads();
        chunk_t* chunk = reinterpret_cast<chunk_t*>(allocator.allocate(sizeof(chunk_t) + sizeof(uint32_t) * num_of_threads, CHUNK_LIST_LEVEL));
        chunk->init(num_of_threads);
        chunk->parent = parent;
        return chunk;
    }

    inline void reclaim_chunk(chunk_t* chunk) { allocator.reclaim(chunk, CHUNK_LIST_LEVEL); }

    inline void delete_chunk(chunk_t* chunk) { allocator.deallocate(chunk, CHUNK_LIST_LEVEL); }

    inline rebalance_object_t* new_ro(chunk_t* f, chunk_t* n) {
        rebalance_object_t* ro = reinterpret_cast<rebalance_object_t*>(allocator.allocate(sizeof(rebalance_object_t), RO_LIST_LEVEL));
        ro->init(f, n);
        return ro;
    }

    inline void reclaim_ro(rebalance_object_t* ro) { allocator.reclaim(ro, RO_LIST_LEVEL); }

    inline void delete_ro(rebalance_object_t* ro) { allocator.deallocate(ro, RO_LIST_LEVEL); }

    inline bool policy_engage(volatile chunk_t* chunk) {
        return ((chunk->i > ((N * 5) >> 3)) || (chunk->i < (N  >> 3))) && flip_a_coin(15);
    }

    inline bool policy_check_rebalance(volatile chunk_t* chunk) {
        return (chunk->i > ((N * 5) >> 3)) && flip_a_coin(5);
    }

    bool check_rebalance(chunk_t* chunk, const K& key) {
        if (chunk->status == INFANT_CHUNK) {
            normalize(chunk->parent, chunk);
            return true;
        }
        if (chunk->i >= N || chunk->status == FROZEN_CHUNK || policy_check_rebalance(chunk)) {
            rebalance(chunk);
            return true;
        }
        return false;
    }

    virtual void rebalance(chunk_t* chunk) {
        // 1. engage
        if (!chunk->ro) {
            // if ro wasn't seted yet create a new ro and try to commit it
            rebalance_object_t *tmp = new_ro(chunk, unset_mark(chunk->next));
            if (!ATOMIC_CAS_MB(&(chunk->ro), nullptr, tmp)) {
                delete_ro(tmp);
            }
        }
        rebalance_object_t* ro = chunk->ro;

        chunk_t* last = chunk;
        while (true) {
            chunk_t* next = unset_mark(ro->next);
            if (next == nullptr || next == &end_sentinel) {
                break;
            }
            if (policy_engage(next)) {
                ATOMIC_CAS_MB(&(next->ro), nullptr, ro);

                if (next->ro == ro) {
                    ATOMIC_CAS_MB(&(ro->next), next, unset_mark(next->next));
                    last = next;
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
        chunk_t* Cn = new_chunk(c);
        chunk_t* Cf = Cn;

        do {
            K arr[N];
            uint32_t count = c->get_keys_to_preserve_from_chunk(arr);
            std::sort(arr, arr + count, compare);
            // the array is now sorted in reverse order - hence we use reverse loop
            for (int j = count - 1; j >= 0; j--) {
                if (Cn->i > (N / 2)) {
                    // Cn is more than half full - create new chunk
                    Cn->min_key = Cn->k[0].key;                     // set Cn min key - this value won't be change
                    Cn->begin_sentinel.next = &Cn->k[0];            // connect begin sentinel to the list
                    Cn->k[Cn->i - 1].next = &(Cn->end_sentinel);    // close the list by point the last key
                                                                    // to the end sentinel
                    Cn->next = new_chunk(ro->first);                // create a new chunk
                    Cn = Cn->next;                                  // Cn points to the new chunk
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
            Cn->min_key = Cn->k[0].key;
            Cn->begin_sentinel.next = &Cn->k[0];
            Cn->k[Cn->i - 1].next = &(Cn->end_sentinel);
        } else {
            // all the chunks are empty - delete the new chunk and set is_empty flag
            delete_chunk(Cn);
            Cf = Cn = nullptr;
            is_empty = true;
        }

        // 5. replace
        do {
            c = last->next;
            if (is_marked(c)) break;
        } while (!ATOMIC_CAS_MB(&(last->next), unset_mark(c), set_mark(c)));

        if (!is_empty) {
            Cn->next = unset_mark(c);
        }

        do {
            chunk_t* pred = load_prev(ro->first);
            if (pred == nullptr) {
                // ro-> first is not accessible - someone else succeeded -
                // delete the chunks we just created and return
                if (!is_empty) {
                    chunk_t* curr = Cf;
                    chunk_t* next;
                    do {
                        next = unset_mark(curr->next);
                        delete_chunk(curr);
                    } while ((curr != Cn) && (curr = next));
                }

                normalize(ro->first, nullptr);
                return;
            }

            if (!is_empty) {
                c = Cf;
            }

            if (ATOMIC_CAS_MB(&(pred->next), unset_mark(ro->first), unset_mark(c))) {
                // success - normalize chunk and free old chunks and normalize
                normalize(ro->first, Cf);

                chunk_t* curr = ro->first;
                chunk_t* next;
                do {
                    next = unset_mark(curr->next);
                    reclaim_chunk(curr);
                } while ((curr != last) && (curr = next));

                reclaim_ro(ro);
                return;
            }

            if (pred->status == FROZEN_CHUNK &&
                unset_mark(pred->next) == ro->first) {
                // the predecessor is being rebalanced - help it and retry
                rebalance(pred);
            }

        } while (true);
    }

    chunk_t* locate_target_chunk(const K& key) {
        if (begin_sentinel.next == &end_sentinel) {
            // the chunk list is empty, we need to create a chunk
            chunk_t* chunk = new_chunk(nullptr);
            chunk->min_key = key;           // set chunk's min key
            chunk->next = &end_sentinel;    // set chunk->next point to end sentinel
            // try to connect the new chunk to the list
            if (ATOMIC_CAS_MB(&(begin_sentinel.next), &end_sentinel, unset_mark(chunk))) {
                // success - normalize
                normalize(nullptr, chunk);
            } else {
                // add failed - delete chunk.
                delete_chunk(chunk);
            }
        }

        chunk_t* c = index.get_pred(key);
        chunk_t* next = unset_mark(c->next);

        while (next != &end_sentinel && !compare(next->min_key, key)) {
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
        chunk_t* prev = index.get_pred(chunk->min_key);
        chunk_t* curr = unset_mark(prev->next);

        while (curr != chunk && curr != &end_sentinel &&
               !compare(curr->min_key, chunk->min_key)) {
            prev = curr;
            curr = unset_mark(prev->next);
        }

        if (curr != chunk) {
            return nullptr;
        }

        return prev;
    }

    void normalize(chunk_t* parent, chunk_t* infant) {
        if (parent) {
            rebalance_object_t *ro = parent->ro;

            chunk_t *curr = parent;
            chunk_t *next;
            // pop out old chunks from index
            do {
                next = unset_mark(curr->next);
                index.pop_conditional(curr->min_key, curr);
                curr = next;
            } while (ro == curr->ro);
        }
        if (infant) {
            chunk_t *curr = infant;
            chunk_t *pred, *next;
            // push new chunks into index and set their status normal
            while (parent == curr->parent && curr->status == INFANT_CHUNK) {
                next = unset_mark(curr->next);
                while (true) {
                    pred = index.get_pred(curr->min_key);
                    if (curr->status != INFANT_CHUNK) break;
                    if (index.push_conditional(curr->min_key, pred, curr)) {
                        ATOMIC_CAS_MB(&(curr->status), INFANT_CHUNK, NORMAL_CHUNK);
                        break;
                    }
                }
                curr = next;
            }
        }
    }

public:

#ifdef GALOIS

    KiWiPQ()
        : allocator(),
          begin_sentinel(),
          end_sentinel(),
          index(allocator, &begin_sentinel){
        begin_sentinel.next = &end_sentinel;
    }

#else

    // constructor for tests only
    KiWiPQ(const K& begin_key,
           const K& end_key)
        : allocator(),
          begin_sentinel(),
          end_sentinel(),
          index(allocator, &begin_sentinel){
        begin_sentinel.next = &end_sentinel;
        begin_sentinel.min_key = begin_key;
        end_sentinel.min_key = end_key;
    }

#endif

    virtual ~KiWiPQ() = default;

    bool push(const K& key) {
        chunk_t* chunk;
        do {
            chunk = locate_target_chunk(key);
        } while(check_rebalance(chunk, key));

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