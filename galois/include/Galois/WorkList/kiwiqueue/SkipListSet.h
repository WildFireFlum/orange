#ifndef __KIWI_SKIP_LIST_SET_H__
#define __KIWI_SKIP_LIST_SET_H__

#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include "Utils.h"

// check cache alignment
template<typename K,typename V>
struct SkipListSetNode {
public:
    // dummy field which is used by the heap when the node is freed.
    // (without it, freeing a node would corrupt a field, possibly affecting
    // a concurrent traversal.)
    void* dummy;

    K key;
    V val;

    int toplevel;
    SkipListSetNode* volatile next[0];

    void init(int _t, SkipListSetNode *_next) {
        toplevel = _t;
        for (int i = 0; i < _t; i++)
            next[i] = _next;
    }

};

template<class Comparer, class Allocator, typename K, typename V>
class LockFreeSkipListSet {

private:
    typedef SkipListSetNode<K,V> sl_node_t;
    Comparer compare;
    Allocator allocator;

public:
    sl_node_t* head;
    uint8_t levelmax;


    //Marsaglia's xorshf generator
    static inline unsigned long xorshf96(unsigned long* x, unsigned long* y, unsigned long* z)  //period 2^96-1
    {
        unsigned long t;
        (*x) ^= (*x) << 16;
        (*x) ^= (*x) >> 5;
        (*x) ^= (*x) << 1;

        t = *x;
        (*x) = *y;
        (*y) = *z;
        (*z) = t ^ (*x) ^ (*y);

        return *z;
    }

    static __thread unsigned long seeds[3];
    static __thread bool seeds_init;

public:
    static inline long rand_range(long r)
    {
        if (!seeds_init) {
            int fd = open("/dev/urandom", O_RDONLY);
            if (read(fd, seeds, 3 * sizeof(unsigned long)) < 0) {
                perror("read");
                exit(1);
            }
            close(fd);
            seeds_init = true;
        }
        long v = xorshf96(seeds, seeds + 1, seeds + 2) % r;
        v++;
        return v;
    }

private:
    void mark_node_ptrs(sl_node_t *n)
    {
        sl_node_t *n_next;
        int i;

        for (i=n->toplevel-1; i>=0; i--)
        {
            do
            {
                n_next = n->next[i];
                if (is_marked(n_next))
                {
                    break;
                }
            } while (!ATOMIC_CAS_MB(&n->next[i], n_next, set_mark(n_next)));
        }
    }


    inline sl_node_t *sl_new_node(int levelmax, sl_node_t *next) {
        sl_node_t *node = reinterpret_cast<sl_node_t *>(allocator.allocate(sizeof(sl_node_t) + levelmax*sizeof(sl_node_t*),  2 /*levelmax-1*/));
        node->init(levelmax, next);
        return node;
    }

    inline sl_node_t *sl_new_node_key(K key, V val, int levelmax) {
        sl_node_t *node = sl_new_node(levelmax, 0);
        node->key = key;
        node->val = val;
        return node;
    }

    inline void sl_delete_node(sl_node_t *n) {
        allocator.deallocate(n, 2 /*n->toplevel-1*/);
    }

public:
    LockFreeSkipListSet() : allocator(), levelmax(23) {
        sl_node_t *min, *max;

        max = sl_new_node(levelmax, NULL);
        min = sl_new_node(levelmax, max);

        head = min;
    }

    bool empty() const
    {
        return head->next[0]->next[0] == 0;
    }

    void fraser_search(K key, sl_node_t **left_list, sl_node_t **right_list, sl_node_t *dead)
    {
        sl_node_t *left, *left_next, *right, *right_next;
        int i;

        retry:
        left = head;
        for (i = (dead ? dead->toplevel : levelmax) - 1; i >= 0; i--)
        {
            left_next = left->next[i];
            if (is_marked(left_next))
                goto retry;
            /* Find unmarked node pair at this level */
            for (right = left_next; ; right = right_next)
            {
                /* Skip a sequence of marked nodes */
                while(1)
                {
                    right_next = right->next[i];
                    if (!is_marked(right_next))
                        break;
                    right = unset_mark(right_next);
                }
                /* Ensure left and right nodes are adjacent */
                if ((left_next != right) &&
                    !ATOMIC_CAS_MB(&left->next[i], left_next, right))
                    goto retry;
                /* When deleting, we have to keep going until we find our target node, or until
                   we observe it has been deleted (right->key > key).  Once this happens, however,
                   we need to descend to a node whose key is smaller than our target's, otherwise
                   we might miss our target in the level below.  (Consider N1 and N2 with the same
                   key, where on level 1, N1 -> N2 but on level 0, N2 -> N1; if when looking for N2
                   we descend at N1, we'll miss N2 at level 0.) */
                if (!right_next || !compare(key, right->key))
                    break;
                left = right;
                left_next = right_next;
            }
            if (left_list != NULL)
                left_list[i] = left;
            if (right_list != NULL)
                right_list[i] = right;
        }
    }

    int get_rand_level()
    {
        int i, level = 1;
        for (i = 0; i < levelmax - 1; i++)
        {
            if ((rand_range(100)-1) < 50)
                level++;
            else
                break;
        }
        /* 1 <= level <= *levelmax */
        return level;
    }

    V get(const K& key) {
        sl_node_t *succs[levelmax], *preds[levelmax];

        fraser_search(key, preds, succs, NULL);
        if (succs[0]->next[0] && succs[0]->key == key)
            return succs[0]->val;
        return static_cast<V>(0);
    }

    sl_node_t* lower_bound(const K& key) {
        sl_node_t *succs[levelmax], *preds[levelmax];

        fraser_search(key, preds, succs, NULL);
        return succs[0];
    }

    bool pop(sl_node_t* node) {
        sl_node_t *first, *next;
        bool result;

        next = node->next[0];
        if (!next || !ATOMIC_CAS_MB(&node->next[0], next, set_mark(next)))
            return false;

        mark_node_ptrs(node);

        fraser_search(node->key, NULL, NULL, node);
        sl_delete_node(node);

        return true;
    }

    bool push(const K& key, const V& val)
    {
        sl_node_t *newn, *new_next, *pred, *succ, *succs[levelmax], *preds[levelmax];
        int i, result = 0;

        newn = sl_new_node_key(key, val, get_rand_level());

        retry:
        fraser_search(key, preds, succs, NULL);
        if (succs[0]->next[0] && succs[0]->key == key)
        {                             /* Value already in list */
            result = 0;
            sl_delete_node(newn);
            goto end;
        }

        for (i = 0; i < newn->toplevel; i++)
        {
            newn->next[i] = succs[i];
        }

        /* Node is visible once inserted at lowest level */
        if (!ATOMIC_CAS_MB(&preds[0]->next[0], succs[0], newn))
        {
            goto retry;
        }

        for (i = 1; i < newn->toplevel; i++)
        {
            while (1)
            {
                pred = preds[i];
                succ = succs[i];
                new_next = newn->next[i];
                /* Give up if pointer is marked */
                if (is_marked(new_next))
                    goto success;
                /* Update the forward pointer if it is stale, which can happen
                   if we called search again to update preds and succs. */
                if (new_next != succ && !ATOMIC_CAS_MB(&newn->next[i], new_next, succ))
                    goto success;
                /* We retry the search if the CAS fails */
                if (ATOMIC_CAS_MB(&pred->next[i], succ, newn)) {
                    if (is_marked(newn->next[i])) {
                        fraser_search(key, NULL, NULL, newn);
                        goto success;
                    }
                    break;
                }

                fraser_search(key, preds, succs, NULL);
            }
        }

        success:
        result = 1;

        end:
        return result;
    }


    V get_pred(const K& key) {
        sl_node_t *succs[levelmax], *preds[levelmax];

        fraser_search(key, preds, succs, NULL);
        return preds[0]->val;
    }

    // pop the node associated with the given key only if the node's value is equal to condition
    bool pop_conditional(const K& key, const V& condition) {
        sl_node_t *succs[levelmax], *preds[levelmax];

        fraser_search(key, preds, succs, NULL);
        if (succs[0]->next[0] && succs[0]->key == key && succs[0]->val == condition) {
            return pop(succs[0]);
        }
        return false;
    }

    bool push_conditional(const K& key, const V& condition, const V& val) {
        sl_node_t *newn, *new_next, *pred, *succ, *succs[levelmax], *preds[levelmax];
        int i, result = 0;

        newn = sl_new_node_key(key, val, get_rand_level());

        retry:
        fraser_search(key, preds, succs, NULL);
        if (succs[0]->next[0] && succs[0]->key == key)
        {                             /* Value already in list - or pred's value is not equal to condition*/
            result = 0;
            sl_delete_node(newn);
            goto end;
        } else if (preds[0]->val != condition) {
            result = 0;
            sl_delete_node(newn);
            goto end;
        }

        for (i = 0; i < newn->toplevel; i++)
        {
            newn->next[i] = succs[i];
        }

        /* Node is visible once inserted at lowest level */
        if (!ATOMIC_CAS_MB(&preds[0]->next[0], succs[0], newn))
        {
            goto retry;
        }

        for (i = 1; i < newn->toplevel; i++)
        {
            while (1)
            {
                pred = preds[i];
                succ = succs[i];
                new_next = newn->next[i];
                /* Give up if pointer is marked */
                if (is_marked(new_next))
                    goto success;
                /* Update the forward pointer if it is stale, which can happen
                   if we called search again to update preds and succs. */
                if (new_next != succ && !ATOMIC_CAS_MB(&newn->next[i], new_next, succ))
                    goto success;
                /* We retry the search if the CAS fails */
                if (ATOMIC_CAS_MB(&pred->next[i], succ, newn)) {
                    if (is_marked(newn->next[i])) {
                        fraser_search(key, NULL, NULL, newn);
                        goto success;
                    }
                    break;
                }

                fraser_search(key, preds, succs, NULL);
            }
        }

        success:
        result = 1;

        end:
        return result;
    }

};

template<class Comparer, class Allocator, typename K, typename V>
__thread unsigned long LockFreeSkipListSet<Comparer, Allocator, K, V>::seeds[3];

template<class Comparer, class Allocator, typename K, typename V>
__thread bool LockFreeSkipListSet<Comparer, Allocator, K, V>::seeds_init;


#endif //__KIWI_SKIP_LIST_SET_H__
