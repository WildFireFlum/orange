#ifndef __PQ_H__
#define __PQ_H__

#include "chunk.h"
#include "index.h"
#include <map>
#include <set>
#include <vector>
#include <algorithm>
#include <thread>

template <typename V>
struct PQ {

    PQ();

    void put(uint32_t key, V& val);


private:
    bool checkRebalance(Chunk<V>& chunk, uint32_t key, V& val);

    void rebalance(Chunk<V>& chunk);

    bool policy(const Chunk<V>& chunk);

    void normalize(Chunk<V>& chunk);

    Chunk<V>& locate_target_chunk(uint64_t key);

    Index m_index;

    std::atomic<uint32_t> m_thread_count;
};

template<typename V>
PQ<V>::PQ() : m_thread_count(0) {}

// Thread local variables..
Chunk* Cn = nullptr;
Chunk* Cf = nullptr;
Chunk* last = nullptr;
uint32_t thread_id = UINT32_MAX;


template<typename V>
void PQ<V>::put(uint32_t key, V &val) {
    uint64_t ukey = (key << 32) | (uint32_t)random();
    Chunk& C = locate_target_chunk(ukey);

    if (checkRebalance(C, key, val)){
        put(key, val);
        return;
    }

    uint32_t i = C.m_count.fetch_add(1);           // allocate cell in linked list

    if (i >= CHUNK_SIZE) {
        // no more free space - trigger rebalance
        rebalance(C);
        put(key, val);
        return;
    }

    if (thread_id == UINT32_MAX) {
        thread_id = m_thread_count.fetch_add(1);
        // TODO: if thread_id >= NUMBER_OF_THREADS we should exit
    }

    // store key, and value
    C.m_v[i] = val;
    C.m_k[i].m_key = ukey;

    PendingPut ppa_t = C.ppa[thread_id];
    if ((ppa_t.m_status != PendingStatus::FROZEN) &&
            !C.ppa[thread_id].compare_exchange_strong(ppa_t, PendingPut(PendingStatus::WORKING, i))) {
        // C is being rebalanced
        rebalance(C);
        put(key, val);
        return;
    }

    while (!C.addToList(C.m_k[i])) {
        // TODO: handle the case we add failed
    }

    // reset ppa_t
    C.ppa[thread_id].store(PendingPut());
}


template<typename V>
bool PQ<V>::checkRebalance(Chunk<V> &C, uint32_t key, V &val) {
    if (C.m_rebalance_status.load() == ChunkStatus::INFANT) {
        // TODO: it is clear why they think it is enough to normalize at that point, but we don't have the required information (Cn, Cf, last are all nullptr...)
        normalize(*C.m_rebalance_parent);
        put(key, val);
        return true;
    }
    if (C.m_count >= CHUNK_SIZE || C.m_rebalance_status.load() == ChunkStatus::FROZEN || policy(C)) {
        rebalance(C);
        put(key, val);
        return true;
    }
    return false;
}


template <typename V>
void PQ<V>::rebalance(Chunk<V> &C) {
    // 1. engage
    RebalanceObject* tmp = new RebalanceObject(C, C.m_next.getRef());
    RebalanceObject* ro_nullptr = nullptr;
    if (!C.m_rebalance_object.compare_exchange_strong(ro_nullptr, tmp)) {
        free(tmp);
    }
    RebalanceObject* ro = C.m_rebalance_object;
    last = &C;
    while (ro->m_next != nullptr) {
        Chunk* next = ro->m_next;
        if (policy(*next)) {
            ro_nullptr = nullptr;
            next->m_rebalance_object.compare_exchange_strong(ro_nullptr, ro);

            if (next->m_rebalance_object == ro) {
                ro->m_next.compare_exchange_strong(next, next->m_next.getRef());
                last = next;
            } else {
                ro->m_next.compare_exchange_strong(next, nullptr);
            }
        } else {
            ro->m_next.compare_exchange_strong(next, nullptr);
        }
    }

    while (last->m_next.getRef()->m_rebalance_object == ro) {
        last = last->m_next.getRef();
    }

    // 2. freeze
    Chunk& c = ro->m_first;
    c.m_rebalance_status = ChunkStatus::FROZEN;
    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
        PendingPut ppa_i = c.ppa[i];
        if (ppa_i.m_status == PendingStatus::BOTTOM) {
            c.ppa[i].compare_exchange_strong(ppa_i, PendingPut(PendingStatus::FROZEN, ppa_i.m_idx));
        }
    }

    // 3. pick minimal version
    // ... we don't have scans so we don't need this part


    // 4. build:
    std::set<KeyElement*> keys;
    Chunk* C0 = &ro->m_first;
    do {

        KeyElement* item = C0->m_begin_sentinel.m_next.getRef();
        while (item != &C0->m_end_sentinel) {
            keys.insert(item);
        }

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            PendingPut& pp = C0->ppa[i];
            if (pp.m_status == PendingStatus::FROZEN) {
                keys.insert(&C0->m_k[i]);
            }
        }

    } while ((C0 != last) && (C0 = C0->m_next.getRef()));

    // TODO: this is incorrect since we delete some of the keys and most probably we delete the min key as well
    Cf = Cn = new Chunk<V>(std::min(keys), &C);
    for (auto k: keys) { //TODO: sort the keys...
        if (Cn->m_count > (CHUNK_SIZE / 2)) {
            Cn->m_next.set(new Chunk<V>(k->m_key, &C), false);
        }
        uint32_t count = Cn->m_count;
        Cn->m_k[count].m_key = k->m_key;
        *Cn->m_k[count].m_value = *k->m_value;
        Cn->m_k[count].m_next.set(&Cn->m_k[count + 1], false);
        Cn->m_count++;
    }


    // 5. replace

    bool curr_mark = false;
    Chunk* curr_ref;
    do {
        curr_ref = last->m_next.getMarkAndRef(curr_mark);
    } while (!curr_mark && !last->m_next.compareAndSet(curr_ref, curr_ref, false, true));


    do {
        Chunk& pred = m_index.loadPrev(C.m_min_key);
        if (pred.m_next.compareAndSet(&C, Cf, false, false)) {
            normalize(C);
            return;
        }

        if (pred.m_next.getRef()->m_rebalance_parent == &C) {
            normalize(C);
            return;
        }

        rebalance(pred);
    } while (true);
}

template<typename V>
Chunk &PQ<V>::locate_target_chunk(uint64_t key) {
    Chunk<V>& C = m_index.loadChunk(key);
    Chunk<V>* next = C.m_next.getRef();

    while ((next != nullptr) && (next->m_min_key <= key)) {
        C = *next;
        next = C.m_next.getRef();
    }

    return C;
}

template<typename V>
bool PQ<V>::policy(const Chunk &C) {
    //TODO: this is an important issue for final tuning
    return (C.m_count > (CHUNK_SIZE * 3 / 4)) || (C.m_count < (CHUNK_SIZE / 4));
}

template<typename V>
void PQ<V>::normalize(Chunk &C) {
    // 6. update index
    RebalanceObject* ro = C.m_rebalance_object;
    Chunk* c = &ro->m_first;
    do {
        m_index.deleteConditional(c->m_min_key, *c);
    } while ((c != last) && (c = c->m_next.getRef()));

    c = Cf;
    do {
        Chunk& prev = m_index.loadPrev(c->m_min_key);
        while (!m_index.putConditional(c->m_min_key, prev, *c)) {
            if (c->m_rebalance_status.load() == ChunkStatus::FROZEN) {
                break;
            }
            prev = m_index.loadPrev(c->m_min_key);
        }
    } while ((c != Cn) && (c = c->m_next.getRef()));

    // 7. normalize

    c = Cf;
    do {
        ChunkStatus stat = c->m_rebalance_status;
        if (stat == ChunkStatus::INFANT) {
            c->m_rebalance_status.compare_exchange_strong(stat, ChunkStatus::NORMAL);
        }
    } while ((c != Cn) && (c = c->m_next.getRef()));

}


#endif //__PQ_H__
