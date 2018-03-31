#include <limits>
#include <ctime>
#include <cstdlib>

#include "chunk.h"

template <typename V>
Chunk<V>::Chunk(uint64_t min_key, Chunk* rebalance_parent)
        : m_min_key(min_key),
          m_next(nullptr),
          KeyElement(std::numeric_limits<uint64_t>::max(), nullptr, nullptr),
          KeyElement(std::numeric_limits<uint64_t>::min(),
                     &m_end_sentinel,
                     nullptr),
          m_rebalance_status(Status::INFANT),
          m_rebalance_parent(rebalance_parent),
          m_rebalance_data(&nullRebalancedData) {
    for (uint32_t i = 0; i < CHUNK_SIZE; i++) {
        m_k[i].m_value = &m_v[i];
        m_k[i].m_key = DELETED_MASK;
    }
    srand((int)time(0));
}

template <typename V>
enum RebalncedCheckResult Chunk<V>::checkRebalance(uint64_t key, V& val) {
    if (m_rebalance_status == Status::INFANT) {
        m_rebalance_parent->normalize();
        return RebalncedCheckResult::NOT_ADDED_KEY_VAL;
    }
    if (m_count >= CHUNK_SIZE || m_rebalance_status == Status::FROZEN ||
        this->policy()) {
        return rebalance(key, val) ? RebalncedCheckResult::ADDED_KEY_VAL
                                   : RebalncedCheckResult::NOT_ADDED_KEY_VAL;
    }
    return RebalncedCheckResult::NOT_REQUIRED;
}

template <typename V>
void Chunk<V>::normalize() {
    // TODO: skip it for now
}

template <typename V>
bool Chunk<V>::policy() {
    // TODO: skip it for now
    return false;
}

template <typename V>
bool Chunk<V>::rebalance(uint64_t key, V& val) {
    // 1. engage
    RebalanceData* ro = new RebalanceData(this, this->m_next);
    if (!this->m_rebalance_data.compare_exchange_strong(nullRebalancedDataPtr,
                                                        ro)) {
        free(ro);
    }
    ro = this->m_rebalance_data;
    Chunk* last = this;
    while (ro->m_next != NULL) {
        Chunk* next = ro->m_next;
        if (next->policy()) {
            next->m_rebalance_data.compare_exchange_strong(
                    nullRebalancedDataPtr, ro);

            if (next->m_rebalance_data == ro) {
                ro->m_next.compare_exchange_strong(next, next->m_next);
                last = next;
            } else {
                ro->m_next.compare_exchange_strong(next, nullptr);
            }
        } else {
            ro->m_next.compare_exchange_strong(next, nullptr);
        }
    }

    while (last->m_next) {
        Chunk* next = last->m_next;
        if (next->m_rebalance_data == ro) {
            last = next;
        } else {
            break;
        }
    }

    // 2. freeze
    Chunk* chunk = ro->m_first;
    while (chunk) {
        chunk->m_rebalance_status = FROZEN;
        for (int i = 0; i < CHUNK_SIZE; i++) {
            // TODO: can and should be improved
            KeyElement* k = &this->m_k[i];
            uint64_t z;
            while (!((z = k->m_key) & FREEZE_MASK) &&
                   !k->m_key.compare_exchange_strong(z, z | FREEZE_MASK))
                ;
        }
    }

    // 3. pick minimal version - (we don't have any ^^)

    // 4. build
    chunk = ro->m_next;
    std::vector<KeyElement> v;

    // add the new key, val
    KeyElement new_key(key, nullptr, &val);
    v.push_back(new_key);

    // add all non deleted keys in the range
    do {
        for (int i = 0; i < CHUNK_SIZE;
             i++) {  // TODO: probably i < min(CHUNK_SIZE, this->m_count) is
            // better than that
            if (!(chunk->m_k[i].m_key & DELETED_MASK)) {
                v.push_back(
                        chunk->m_k[i]);  // TODO: maybe we don't want to copy them
                // at this point
            }
        }
    } while ((chunk != last) && (chunk = chunk->m_next));

    // sort the keys
    std::sort(v.begin(), v.end());

    // create new chunk TODO: should use memory allocation mechanism
    Chunk* Cn = new Chunk(v[0].m_key, this);
    Cn->m_begin_sentinel.m_next = &Cn->m_k[0];
    Chunk* Cf = Cn;

    // arrange the keys (and values) in a list of new chunks
    for (auto& k : v) {
        if (Cn->m_count > (CHUNK_SIZE / 2)) {
            // more than half full - create new one
            Cn->m_k[Cn->m_count - 1].m_next = &Cn->m_end_sentinel;
            Cn->m_next = new Chunk(k.m_key, this);
            Cn = Cn->m_next;
            Cn->m_begin_sentinel.m_next = &Cn->m_k[0];
        }
        uint64_t tmp = k.m_key;
        Cn->m_k[Cn->m_count].m_key =
                tmp & ~ALL_FLAGS_MASK;                   // set key without flags
        *Cn->m_k[Cn->m_count].m_value = *k.m_value;  // copy the value
        Cn->m_k[Cn->m_count].m_next =
                &Cn->m_k[Cn->m_count + 1];  // set list pointer
        Cn->m_count++;
    }

    // 5. replace
    // TODO: true and false for atomic markable reference shit...
    // in general this part is not so clear - pred ? help rebalance pred?
    do {
        Cn->m_next = last->m_next;
    } while (!last->m_next.compare_exchange_strong(Cn->m_next + false,
                                                   Cn->m_next + true));

    Chunk* pred = this predecessor;  // TODO: read in the paper how they get the
    // predecessor...

    do {
        if (pred->m_next.compare_exchange_strong(this + false, Cf + false)) {
            this->normalize();
            return true;
        }

        if ((chunk = pred->m_next)->m_rebalance_parent == this) {
            this->normalize();
            return false;
        }
        // TODO: support rebalance without key, val
        pred->rebalance();
    } while (true);
}

template <typename V>
bool Chunk<V>::put(uint32_t key, V& val) {
    switch (checkRebalance(key, val)) {
        case RebalncedCheckResult::NOT_REQUIRED:
            break;
        case RebalncedCheckResult::ADDED_KEY_VAL:
            return true;
        case RebalncedCheckResult::NOT_ADDED_KEY_VAL:
            return false;
    }

    // TODO: init random seed...
    uint64_t unique_key = (key << 32 | (uint32_t)rand()) & ~ALL_FLAGS_MASK;
    uint32_t i = m_count.fetch_add(1);
    if (i >= CHUNK_SIZE) {
        // no more free space - trigger rebalance
        return rebalance(unique_key, val);
    }
    m_v[i] = val;  // TODO: CAS is not needed but we need something like
    // volatile in java here
    uint64_t ithKey = m_k[i].m_key;

    if ((ithKey & FREEZE_MASK) ||
        !m_k[i].m_key.compare_exchange_strong(ithKey, unique_key)) {
        return rebalance(unique_key, val);
    }

    // TODO: Make sure we dont want to loop over until we succeed
    return addToList(&m_k[i]);
}

template <typename V>
bool Chunk<V>::remove_from_list(KeyElement<V>* key) {
    while (true) {
        KeyElement* pred;
        KeyElement* curr;

        {
            auto elemWithKeyWindow = this->find(key->m_key);
            pred = elemWithKeyWindow.first;
            curr = elemWithKeyWindow.second;
        }

        if (curr->m_key != key->m_key) {
            return false;
        } else {
            auto succ = curr->m_next.getRef();
            if (!curr->m_next.attemptMark(succ, true)) {
                continue;
            }
            pred->m_next.compareAndSet(curr, succ, false, false);
            return true;
        }
    }
}

template <typename V>
bool Chunk<V>::delMin(std::pair<uint32_t, V>& out) {
    if (m_rebalance_status == Status::FROZEN) {
        return false;  // consider joining the rebalance
    }

    auto curr = m_begin_sentinel.m_next.getRef();
    while (curr < &m_end_sentinel) {
        uint64_t currKey = curr->m_key;
        if (!(currKey & ALL_FLAGS_MASK) &&
            curr->m_key.compare_exchange_strong(currKey,
                                                currKey | DELETED_MASK)) {
            out.first = (uint32_t)(currKey >> 32);
            out.second = *curr->m_value;
            remove_from_list(curr);
            return true;
        }
        currKey = curr->m_key;
        if (currKey & FREEZE_MASK) {
            return false;  // consider joining the rebalance
        }

        // skip deleted
        curr = curr->m_next.getRef();
    }

    return false;
}

template <typename V>
std::pair<KeyElement*, KeyElement*> Chunk<V>::find(uint64_t key) {
    KeyElement* pred = nullptr;
    KeyElement* curr = nullptr;
    KeyElement* succ = nullptr;

    retry:
    while (true) {
        pred = &m_begin_sentinel;
        curr = pred->m_next.getRef();
        while (true) {
            succ = curr->m_next.getRef();
            bool marked = true;
            while (marked) {
                if (!pred->m_next.compareAndSet(curr, succ, false, false)) {
                    goto retry;
                }
                curr = succ;
                succ = curr->m_next.getMarkAndRef(marked);
            }
            if (curr->m_key >= key) {
                return std::pair<KeyElement*, KeyElement*>(pred, curr);
            }
            pred = curr;
            curr = succ;
        }
    }
}

template <typename V>
bool Chunk<V>::addToList(KeyElement<V>* item) {
    auto key = item->m_key.load();
    while (true) {
        auto prevNextPair(this->find(key));
        KeyElement<V>* pred = prevNextPair.first;
        KeyElement<V>* curr = prevNextPair.second;
        if (curr->m_key == key) {
            return false;
        } else {
            item->m_next.set(curr, false);
            if (pred->m_next.compareAndSet(curr, item, false, false)) {
                return true;
            }
        }
    }
}