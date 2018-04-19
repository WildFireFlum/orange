#ifndef __KIWI_SKIP_LIST_SET_H__
#define __KIWI_SKIP_LIST_SET_H__


#include "Utils.h"

// check cache alignment
template<typename K, typename V>
struct IndexNode {
public:
    // dummy field which is used by the heap when the node is freed.
    // (without it, freeing a node would corrupt a field, possibly affecting
    // a concurrent traversal.)
    void* dummy;

    K key;
    V val;

    int toplevel;
    IndexNode* volatile next[0];

    void init(int _t, IndexNode *_next) {
        toplevel = _t;
        for (int i = 0; i < _t; i++)
            next[i] = _next;
    }

};

template<class Comparer, class Allocator, typename K, typename V>
class Index {

protected:
    typedef IndexNode<K, V> sl_node_t;

    Allocator& allocator;
    Comparer compare;

    sl_node_t* head;
    uint8_t levelmax;


protected:
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
        auto *node = reinterpret_cast<sl_node_t *>(allocator.allocate(sizeof(sl_node_t) + levelmax*sizeof(sl_node_t*), levelmax-1));
        node->init(levelmax, next);
        return node;
    }

    inline sl_node_t *sl_new_node_key(K key, V val, int levelmax) {
        auto *node = sl_new_node(levelmax, 0);
        node->key = key;
        node->val = val;
        return node;
    }

    inline void sl_reclaim_node(sl_node_t *n) {
        allocator.reclaim(n, n->toplevel-1);
    }

public:

    Index(Allocator& r_allocator, const V& val) : allocator(r_allocator), levelmax(INDEX_SKIPLIST_LEVELS) {
        sl_node_t *min, *max;

        max = sl_new_node(levelmax, NULL);
        min = sl_new_node(levelmax, max);

        head = min;
        head->val = val;
    }

    void fraser_search(const K& key, sl_node_t **left_list, sl_node_t **right_list, sl_node_t *dead)
    {
        sl_node_t *left, *left_next, *right, *right_next;
        int i;

        retry:
        left = head;
        for (i = (dead ? dead->toplevel : levelmax) - 1; i >= 0; i--)
        {
            sl_node_t *first = NULL;

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
                if (left_next != right) {
                    if (!ATOMIC_CAS_MB(&left->next[i], left_next, right))
                        goto retry;
                    for (sl_node_t *t = left_next; t != right; t = unset_mark(t->next[i]))
                        t->next[i] = set_dead(t->next[i]);
                }
                /* When deleting, we have to keep going until we find our target node, or until
                   we observe it has been deleted (right->key > key).  Once this happens, however,
                   we need to descend to a node whose key is smaller than our target's, otherwise
                   we might miss our target in the level below.  (Consider N1 and N2 with the same
                   key, where on level 1, N1 -> N2 but on level 0, N2 -> N1; if when looking for N2
                   we descend at N1, we'll miss N2 at level 0.) */
                if (!dead) {
                    if (!right_next || !compare(key, right->key))
                        break;
                } else {
                    if (!first && !compare(key, right->key))
                        first = left;
                    if (!right_next || is_dead(dead->next[i]) || compare(right->key, key)) {
                        if (first) left = first;
                        break;
                    }
                }
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

    bool push_conditional(const K& key, const V& prev, const V& val)
    {
        sl_node_t *newn, *new_next, *pred, *succ, *succs[levelmax], *preds[levelmax];
        bool result;

        newn = sl_new_node_key(key, val, get_rand_level());

        retry:
        fraser_search(key, preds, succs, NULL);
        if (succs[0]->key == key || preds[0]->val != prev)
        {                             /* Value already in list */
            result = false;
            sl_reclaim_node(newn);
            goto end;
        }

        for (int i = 0; i < newn->toplevel; i++)
        {
            newn->next[i] = succs[i];
        }

        /* Node is visible once inserted at lowest level */
        if (!ATOMIC_CAS_MB(&preds[0]->next[0], succs[0], newn))
        {
            goto retry;
        }

        for (int i = 1; i < newn->toplevel; i++)
        {
            while (true)
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
        result = true;

    end:
        return result;
    }


    bool complete_pop(sl_node_t *first)
    {
        sl_node_t *next = first->next[0];

        if (is_marked(next) ||
            !ATOMIC_CAS_MB(&first->next[0], next, set_mark(next)))
            return false;

        mark_node_ptrs(first);

        fraser_search(first->key, NULL, NULL, first);
        sl_reclaim_node(first);

        return true;
    }

    // in case the key is in
    V& get_pred(const K& key) {
        sl_node_t *succs[levelmax], *preds[levelmax];
        fraser_search(key, preds, succs, nullptr);
        return preds[0]->val;
    }

    bool pop_conditional(const K& key, const V& val) {
        sl_node_t *succs[levelmax], *preds[levelmax];
        fraser_search(key, preds, succs, nullptr);
        if (succs[0]->next[0] && succs[0]->key == key && succs[0]->val == val) {
            return complete_pop(succs[0]);
        }
    }

};

#endif //__KIWI_SKIP_LIST_SET_H__
