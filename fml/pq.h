#ifndef __PQ_H__
#define __PQ_H__

#include "chunk.h"

template <typename V>
struct PQ {

    PQ();

    RebalncedCheckResult checkRebalance(Chunk<V>& chunk, uint32_t key, V& val);

    bool rebalance(Chunk<V>& chunk, std::pair<uint32_t, V>* keyValPair);

    void put(uint32_t key, V& val);

    bool delete_min(std::pair<uint32_t, V>* out);

    Chunk m_begin_sentinel;
    Chunk m_end_sentinel;
};

template<typename V>
PQ<V>::PQ() : m_begin_sentinel(0, nullRebalancedDataPtr), m_end_sentinel(UINT64_MAX, nullptr) {
    this->m_begin_sentinel.m_next = &this->m_end_sentinel;
}

template<typename V>
void PQ<V>::put(uint32_t key, V &val) {
    Chunk* chunk;
    PutStatus putStatus;
    while (true) {
        //TODO this is not good enough since we can't rebalance m_begin_sentinal
        chunk = &this->m_begin_sentinel;
        while (chunk->m_min_key > key) chunk = chunk->m_next.getRef();

        switch (checkRebalance(*chunk, key, val)) {
            case RebalncedCheckResult::NOT_REQUIRED:
                break;
            case RebalncedCheckResult::ADDED_KEY_VAL:
                return;
            case RebalncedCheckResult::NOT_ADDED_KEY_VAL:
                break;
        }
        putStatus = chunk->put(key, val);
        if (putStatus == PutStatus::NEED_REBALANCE) {
            auto pair = std::pair<uint32_t, V>(key, val);
            if (rebalance(*chunk, &pair)) {
                return;
            }
        }
    }
}

template<typename Val>
bool PQ<Val>::delete_min(std::pair<uint32_t, Val &> *out) {

    return false;
}

template<typename V>
RebalncedCheckResult PQ<V>::checkRebalance(Chunk<V> &chunk, uint32_t key, V &val) {
    if (chunk.m_rebalance_status == Status::INFANT) {
        chunk.m_rebalance_parent->normalize();
        return RebalncedCheckResult::NOT_ADDED_KEY_VAL;
    }
    if (chunk.m_count >= CHUNK_SIZE || chunk.m_rebalance_status == Status::FROZEN ||
            chunk.policy()) {
        auto pair = std::pair(key, val);
        return rebalance(chunk, &pair) ? RebalncedCheckResult::ADDED_KEY_VAL
                                : RebalncedCheckResult::NOT_ADDED_KEY_VAL;
    }
    return RebalncedCheckResult::NOT_REQUIRED;
}


template <typename V>
bool PQ<V>::rebalance(Chunk<V> &C, std::pair<uint32_t, V> *keyValPair) {
    // 1. engage
    RebalanceData* ro = new RebalanceData(&C, C.m_next.getRef());
    if (!C.m_rebalance_data.compare_exchange_strong(nullRebalancedDataPtr,
                                                        ro)) {
        free(ro);
    }
    ro = C.m_rebalance_data;
    Chunk* last = &C;
    while (ro->m_next != nullptr) {
        Chunk* next = ro->m_next;
        if (next->policy()) {
            next->m_rebalance_data.compare_exchange_strong(nullRebalancedDataPtr, ro);

            if (next->m_rebalance_data == ro) {
                ro->m_next.compare_exchange_strong(next, next->m_next.getRef());
                last = next;
            } else {
                ro->m_next.compare_exchange_strong(next, nullptr);
            }
        } else {
            ro->m_next.compare_exchange_strong(next, nullptr);
        }
    }

    while (last->m_next.getRef()->m_rebalance_data == ro) {
        last = last->m_next.getRef();
    }

    // 2. freeze
    Chunk* chunk = ro->m_first;
    while (chunk) {
        chunk->m_rebalance_status = Status::FROZEN;
        for (int i = 0; i < CHUNK_SIZE; i++) {
            // TODO: can and should be improved
            KeyElement* currElem = &C.m_k[i];
            uint64_t currKey;
            while (!((currKey = currElem->m_key) & FREEZE_MASK) &&
                   !currElem->m_key.compare_exchange_strong(currKey, currKey | FREEZE_MASK));
        }
    }

    // 3. pick minimal version - (we don't have any ^^)

    // 4. build:
    chunk = ro->m_next;
    // TODO: size can be calculated - we need to check if it is more efficient or not...
    std::vector<KeyElement> v;

    // add key val pair (if not nullptr)
    if (keyValPair) {
        v.push_back(KeyElement(keyValPair->first << KEY_OFFSET, nullptr, &keyValPair->second));
    }

    // add all non deleted keys in the range
    do {
        for (int i = 0; i < CHUNK_SIZE;
             i++) {  // TODO: probably i < min(CHUNK_SIZE, this->m_count) is better
            // than that
            if (!(chunk->m_k[i].m_key & DELETED_MASK)) {
                v.push_back(chunk->m_k[i]);  // TODO: maybe we don't want to copy them
                // at this point
            }
        }
    } while ((chunk != last) && (chunk = chunk->m_next.getRef()));

    // sort the keys
    std::sort(v.begin(), v.end());

    // create new chunk TODO: should use memory allocation mechanism
    Chunk* Cn = new Chunk(v[0].m_key, this);
    Cn->m_begin_sentinel.m_next.set(&Cn->m_k[0], false);
    Chunk* Cf = Cn;

    // arrange the keys (and values) in a list of new chunks
    for (auto& k : v) {
        if (Cn->m_count > (CHUNK_SIZE / 2)) {
            // more than half full - create new one
            Cn->m_k[Cn->m_count - 1].m_next.set(&Cn->m_end_sentinel, false);
            //TODO should be reclaimable memory
            Cn->m_next.set(new Chunk(k.m_key, this), false);
            Cn = Cn->m_next.getRef();
            Cn->m_begin_sentinel.m_next.set(&Cn->m_k[0], false);
        }
        uint32_t count = Cn->m_count;
        uint64_t unique_key = (k.m_key & KEY_MASK) | ((uint32_t)(count << FLAGS_BIT_OFFSET));
        KeyElement& keyElement = Cn->m_k[count];
        keyElement.m_key = unique_key;                     // set key without flags
        *keyElement.m_value = *k.m_value;                  // copy the value
        keyElement.m_next.set(&Cn->m_k[count + 1], false); // set list pointer
        Cn->m_count++;
    }

    // 5. replace

    bool curr_mark = false;
    Chunk* curr_ref;
    do {
        curr_ref = last->m_next.getMarkAndRef(curr_mark);
    } while (!last->m_next.compareAndSet(curr_ref, curr_ref, false, true));

    Chunk* pred = this predecessor;  // TODO: read in the paper how they get the
    // predecessor...

    do {
        if (pred->m_next.compareAndSet(this, Cf, false, false)) {
            C.normalize();
            return true;
        }

        if (pred->m_next.getRef()->m_rebalance_parent == this) {
            C.normalize();
            return false;
        }
        // TODO: support rebalance without key, val
        pred->rebalance(nullptr);
    } while (true);
}



#endif //__PQ_H__
