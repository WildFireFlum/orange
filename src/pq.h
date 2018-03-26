#ifndef __PQ_H__
#define __PQ_H__

#include <bits/shared_ptr.h>
#include <vector>

namespace Galois {
/**
 * Scheduling policies for Galois iterators. Unless you have very specific
 * scheduling requirement, {@link dChunkedLIFO} or {@link dChunkedFIFO} is a
 * reasonable scheduling policy. If you need approximate priority scheduling,
 * use {@link OrderedByIntegerMetric}. For debugging, you may be interested
 * in {@link FIFO} or {@link LIFO}, which try to follow serial order exactly.
 *
 * The way to use a worklist is to pass it as a template parameter to
 * {@link for_each()}. For example,
 *
 * \code
 * Galois::for_each<Galois::WorkList::dChunkedFIFO<32> >(begin, end, fn);
 * \endcode
 */
namespace WorkList {
namespace {  // don't pollute the symbol table with the example

// Worklists may not be copied.
// Worklists should be default instantiatable
// All classes (should) conform to:
template <typename T, bool Concurrent>
class AbstractWorkList {
  AbstractWorkList(const AbstractWorkList&);
  const AbstractWorkList& operator=(const AbstractWorkList&);

 public:
  AbstractWorkList() {}

  //! T is the value type of the WL
  typedef T value_type;

  //! change the concurrency flag
  template <bool _concurrent>
  struct rethread {
    typedef AbstractWorkList<T, _concurrent> type;
  };

  //! change the type the worklist holds
  template <typename _T>
  struct retype {
    typedef AbstractWorkList<_T, Concurrent> type;
  };

  //! push a value onto the queue
  void push(const value_type& val);

  //! push a range onto the queue
  template <typename Iter>
  unsigned int push(Iter b, Iter e);

  //! push initial range onto the queue
  //! called with the same b and e on each thread
  template <typename RangeTy>
  unsigned int push_initial(const RangeTy&);

  //! pop a value from the queue.
  /*Galois::optional<value_type>*/ void pop();
};

}  // namespace
}  // end namespace WorkList
}  // end namespace Galois

#endif  //__PQ_H__
