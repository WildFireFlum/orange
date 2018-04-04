#ifndef __ATOMIC_MARKABLE_REFERENCE_H__
#define __ATOMIC_MARKABLE_REFERENCE_H__

#include <assert.h>
#include <atomic>

template <class T>
class AtomicMarkableReference {

public:
    AtomicMarkableReference(T* ref, bool mark) : m_val(combine(ref, mark)) {
        assert((reinterpret_cast<uintptr_t>(ref) & MASK) == 0 &&
               "only works with pointers that have the low bit cleared");
    }

    AtomicMarkableReference(AtomicMarkableReference& other) = delete;

    /////// Getters
    T* getRef() {
        return reinterpret_cast<T*>(m_val.load(DEFAULT_ORDER) & ~MASK);
    }

    bool isMarked() { return (m_val.load(DEFAULT_ORDER) & MASK); }

    T* getMarkAndRef(bool& mark) {
        uintptr_t current = m_val.load(DEFAULT_ORDER);
        mark = current & MASK;
        return reinterpret_cast<T*>(current & ~MASK);
    }

    /////// Setters (and exchange)

    bool attemptMark(T* expectedVal, bool newMark) {
        uintptr_t oldBoth = m_val;
        uintptr_t newBoth = (m_val & ~MASK) | newMark;
        return m_val.compare_exchange_strong(oldBoth, newBoth);
    }

    bool compareAndSet(T* expectedVal,
                       T* newVal,
                       bool expectedMark,
                       bool newMark) {
        uintptr_t expectedBoth =
                (reinterpret_cast<uintptr_t>(expectedVal) & ~MASK) | expectedMark;
        uintptr_t newBoth = (reinterpret_cast<uintptr_t>(newVal) & ~MASK) | newMark;
        return m_val.compare_exchange_strong(expectedBoth, newBoth);
    }

    void set(T* newVal, bool newMark) { m_val = combine(newVal, newMark); }

private:

    constexpr uintptr_t combine(T* ref, bool mark) {
        return reinterpret_cast<uintptr_t>(ref) | mark;
    }

    std::atomic<uintptr_t> m_val;
    static constexpr uintptr_t MASK = 1;
    static constexpr std::memory_order DEFAULT_ORDER = std::memory_order_seq_cst;

};

#endif  // __ATOMIC_MARKABLE_REFERENCE_H__
