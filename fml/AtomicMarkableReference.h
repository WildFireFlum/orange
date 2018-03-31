//
// Created by Ynon on 31/03/2018.
//

#ifndef ORANGE_ATOMICMARKABLEREFERENCE_H
#define ORANGE_ATOMICMARKABLEREFERENCE_H

#include <assert.h>
#include <atomic>

template <class T>
class MarkableReference {
 private:
  std::atomic<T*> m_val;
  static constexpr uintptr_t MASK = 1;
  static constexpr std::memory_order DEFAULT_ORDER = std::memory_order_seq_cst;

  constexpr uintptr_t combine(T* ref, bool mark) {
    return reinterpret_cast<uintptr_t>(ref) | mark;
  }

 public:
  MarkableReference(T* ref, bool mark) : m_val(combine(ref, mark)) {
    // note that construction of an atomic is not *guaranteed* to be atomic, in
    // case that matters. On most real CPUs, storing a single aligned
    // pointer-sized integer is atomic This does mean that it's not a seq_cst
    // operation, so it doesn't synchronize with anything (and there's no MFENCE
    // required)
    assert((reinterpret_cast<uintptr_t>(ref) & mask) == 0 &&
           "only works with pointers that have the low bit cleared");
  }

  MarkableReference(MarkableReference& other) = delete;

  /////// Getters
  T* getRef() { return reinterpret_cast<T*>(val.load(DEFAULT_ORDER) & ~mask); }

  bool getMark() { return (val.load(DEFAULT_ORDER) & mask); }

  T* getMarkAndRef(bool& mark) {
    uintptr_t current = val.load(DEFAULT_ORDER);
    mark = expected & mask;
    return reinterpret_cast<T*>(expected & ~mask);
  }

  /////// Setters (and exchange)
  bool compareAndSet(T* expectedVal,
                     T* newVal,
                     bool expectedMark,
                     bool newMark) {
    uintptr_t expectedBoth = (expectedVal & ~mask) | expectedMark;
    uintptr_t newBoth = (newVal & ~mask) | newMark;
    return m_val.compare_exchange_strong(expectedBoth, newBoth);
  }

  bool cmpxchgBoth_weak(T*& expectRef,
                        bool& expectMark,
                        T* desiredRef,
                        bool desiredMark,
                        std::memory_order order = std::memory_order_seq_cst) {
    uintptr_t desired = combine(desiredRef, desiredMark);
    uintptr_t expected = combine(expectRef, expectMark);
    bool status = compare_exchange_weak(expected, desired, order);
    expectRef = reinterpret_cast<T*>(expected & ~mask);
    expectMark = expected & mask;
    return status;
  }

  void setRef(T* ref, std::memory_order order = std::memory_order_seq_cst) {
    xchgReg(ref, order);
  }  // I don't see a way to avoid cmpxchg without a non-atomic
  // read-modify-write of the boolean.
  void setRef_nonatomicBoolean(
      T* ref,
      std::memory_order order = std::memory_order_seq_cst) {
    uintptr_t old = val.load(std::memory_order_relaxed);  // maybe provide a way
    // to control this
    // order?
    // !!modifications to the boolean by other threads between here and the
    // store will be stepped on!
    uintptr_t newval = combine(ref, old & mask);
    val.store(newval, order);
  }

  void setMark(bool mark, std::memory_order order = std::memory_order_seq_cst) {
    if (mark)
      val.fetch_or(mask, order);
    else
      val.fetch_and(~mask, order);
  }

  bool toggleMark(std::memory_order order = std::memory_order_seq_cst) {
    return mask & val.fetch_xor(mask, order);
  }

  bool xchgMark(bool mark,
                std::memory_order order = std::memory_order_seq_cst) {
    // setMark might still compile to efficient code if it just called this and
    // let the compile optimize away the fetch part
    uintptr_t old;
    if (mark)
      old = val.fetch_or(mask, order);
    else
      old = val.fetch_and(~mask, order);
    return (old & mask);
    // It might be ideal to compile this to x86 BTS or BTR instructions (when
    // the old value is needed) but clang uses a cmpxchg loop.
  }
};

#endif  // ORANGE_ATOMICMARKABLEREFERENCE_H
