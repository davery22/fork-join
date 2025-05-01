# Fork-Join Data Structures

Mutable data structures with cheap copy ("fork") and merge ("join") operations. These operations are so-named in
reference to the JDK's own fork-join concurrency framework that underpins parallel streams, which these data structures
were intended to integrate with.

<strong>Use at your own risk</strong>. While there are a number of unit tests, there is a lack of scenario tests, and
real potential for non-obvious interactions. There is also a lack of thorough benchmarking and performance analysis.

---

If you are not coming from the [blog post](https://daniel.avery.io/writing/fork-join-data-structures), you may want see
that for a quick introduction - it's a short read.

---

## Adding Fork-Join to your build

Fork-Join's Maven group ID is `io.avery`, and its artifact ID is `util.forkjoin`.

To add a dependency on Fork-Join using Maven, use the following:

```xml
<dependency>
  <groupId>io.avery</groupId>
  <artifactid>util.forkjoin</artifactid>
  <version>0.1</version>
</dependency>
```

## Quick Overview

Currently, there is only one data structure here: `TrieForkJoinList`. It implements the only interface here,
`ForkJoinList`, which fits comfortably on a postcard:

``` java
public interface ForkJoinList<E> extends List<E> {
    ForkJoinList<E> fork();
    ForkJoinList<E> subList();
    ForkJoinList<E> reversed();
    boolean join(Collection<? extends E> c);
    boolean join(int index, Collection<? extends E> c);
}
```

The `fork()` method returns a copy of the list.

The `subList()` and `reversed()` views are overridden from the superinterface to return a `ForkJoinList`. 

The `join()` methods look suspiciously similar to the `addAll()` methods on `List`. In fact, the only difference between
them is that the `join()` methods will `fork()` the argument passed to them if it happens to be a `ForkJoinList`, and
then use a sublinear algorithm to integrate the result, if possible. If not, `join()` falls back to `addAll()`, which
is a linear / element-wise operation.

Since `ForkJoinList` extends `List`, a `TrieForkJoinList` can be used as a drop-in replacement implementation for any
`List`, and it will "just work"*.

*Almost. Due to the nature of structural sharing, in certain situations, `set()` has to be considered a "structural
modification". This means that an iterator will throw `ConcurrentModificationException` if we attempt to reuse it after
calling `set()` on another iterator, or the list. These "certain situations" won't occur if we never use `fork()` or
`join()` - but then, what's the point?

## Implementation Details

TrieForkJoinList is an implementation of a Relaxed Radix Balanced (RRB) Tree, drawing primarily from the
[technical report](https://infoscience.epfl.ch/server/api/core/bitstreams/e5d662ea-1e8d-4dda-b917-8cbb8bb40bf9/content)
by Bagwell and Rompf, and subsequent [master's thesis](https://hypirion.com/thesis.pdf) by L'orange. RRB trees extend
the better-known "persistent vector" (radix balanced tree) with O(logN) concatenation and split operations, mainly by
"relaxing" the density requirement of the tree (allowing interior nodes to be non-full). This introduces a notion of
"balance", addressing how node children should be redistributed during operations like concatenation to trade off speed
of the operation vs node density (which affects speed of subsequent operations). Sparse subtrees track the cumulative
sizes of their children in a size table, to support "relaxed" radix indexing.

Basic features of TrieForkJoinList:
1. We use a branching factor (maximum node width) of 32.
2. We use the "search step invariant" from the literature for rebalancing: During concatenation, adjacent subtrees are
   considered balanced if the total number of direct children is within a tolerance (currently 2) of the minimum
   number needed to contain the total number of grandchildren. (This bounds the number of size table comparisons
   / "search steps" at each node during relaxed radix indexing.)
3. We use a tail for faster operations at the end of the list, as described by L'orange.
4. We trim interior nodes to size; only the tail is allowed to have capacity greater than the number of elements it
   contains. This reduces standing memory usage, but means that operations that grow or shrink interior nodes must copy
   arrays.

Distinguishing features of TrieForkJoinList:
1. Rather than forcing update operations to return a new instance (copy + mutate, as in "persistent" data
   structures), we separate a copy operation (fork) from mutative operations, allowing the implementation to efficiently
   conform to the java.util.List interface (including all optional operations). The copy operation is O(1) when copying
   a full list, or O(logN) when copying a sublist (aka 'slicing' or 'splitting' in the literature).
2. Rather than having each node reference a list id to determine if it is owned or shared (as in L'orange's
   transient variant), we have each node keep a bitset tracking which of its children are owned or
   shared. A node is owned if its parent is owned and its bit is set in its parent's bitset. <b>An owned node does not
   need to be copied prior to mutation.</b> This finer-grained ownership tracking allows a list to share nodes with a
   sublist fork while retaining ownership of nodes outside the sublist fork. Tracking ownership on the parent instead
   of on each child reduces data fetches, and allows for bitwise operations to bulk-update ownership when many children
   are moved, as during concatenation. For a branching factor of 32, each bitset takes up the same space as a 4-byte
   pointer to a list id would.
3. We do not use a planning phase during rebalancing, preferring to optimize for the YAGNI case instead. This can lead
   to redundant copying in some cases, but does not appear to be a bottleneck. In general, we try to postpone copying in
   order to fuse with other operations, within "reason" (depending on your tolerance for code sprawl / complexity).
4. We compress size tables at lower levels in the tree, taking advantage of the lower maximum
   cumulative size to use narrower primitive types (e.g. byte or char instead of int).
5. We attempt to avoid or even eliminate existing size tables during operations like concatenation.
6. For the "addAll" methods and bulk-load constructor (which cannot leverage logN concatenation), we use an optimized
   algorithm which pre-allocates nodes and inserts in node-sized batches from the bottom of the tree, minimizing
   traversal overhead.
7. We implement iterators to maintain their own internal node stack for optimized traversal, updates, and inserts.
   In addition, ListIterator.add() skips rebalancing and inserts directly into its current leaf node if the
   leaf is not full (on the basis that this operation strictly increases the density of the tree).
8. Like java.util.ArrayList, we track structural modifications to ensure iterators fail-fast in the case of concurrent
   modification.

## Q&A

I can't promise I have made only good decisions, but I will try to explain my reasoning.

---

<details><summary><b>1. Why not implement <code>Cloneable</code> and override <code>clone()</code>, instead of introducing <code>fork()</code>?</b></summary>

The main reason is that `fork()` may need to change owned nodes to shared, which is a mutation to the original
list. It is not clear that mutating the original list would not violate user expectations of `clone()` (even though
this mutation does not affect "observable results" of subsequent operations on the original list - only their performance).

Even if `Cloneable`/`clone()` were used in place of `fork()`, we would still need an interface above `TrieForkJoinList`
that extends `Cloneable`, so that the `subList()` and `reversed()` views could still be overridden to return something
that is `Cloneable` & `List`.

</details>

---

<details><summary><b>2. Why not implement logN concatenation in <code>addAll()</code>, instead of introducing <code>join()</code>?</b></summary>

The performance and memory characteristics of `addAll()` vs `join()` are "different", not strictly "worse/better".
`addAll()` is a linear operation because it eagerly copies the passed-in list, whereas `join()` achieves sublinear cost
(when the argument is compatible) by deferring most copying until subsequent mutations. If most of the copied region
will later be mutated, it is possible that "eager copying" (`addAll()`) will outperform "lazy copying" (`join()`) in the
long run.

Additionally, logN concatenation requires reusing internal structure from the passed-in list, which implies either
"sharing" or "stealing" that structure - both of which imply mutating the passed-in list (see Q&A 1 and 4). It is not
clear that this mutation would not violate user expectations of `addAll()`.

</details>

---

<details><summary><b>3. Why not require <code>join()</code> to accept a <code>ForkJoinList</code> / some type that better ensures logN concatenation?</b></summary>

Even if `join()` required a `ForkJoinList` argument, it would not be sufficient to guarantee logN concatenation -
different implementations of the interface would not be able to access each other's internal structure for reuse, and
would likely have incompatible internal structure anyway.

We could instead add a "recursive" type parameter to `ForkJoinList` (representing the type of 'this'), and have `join()`
accept that type. But that seems too restrictive: For instance, it should be possible to `join()` a subList or reversed
view, but such a view will likely not be the same type as the outer list. Not to mention, a recursive type parameter
makes the interface type much more difficult to work with in all places that the interface needs to be spelled out
explicitly.

On the other hand, accepting a `Collection` argument allows us to use `join()` in place of `addAll()` (if we are
comfortable with its "lazy copying" and potential mutation - see Q&A 2), and gracefully degrade to a linear algorithm if
necessary.

</details>

---

<details><summary><b>4. Why does <code>join()</code> call <code>fork()</code> on its argument?</b></summary>

For logN concatenation to be possible, the joining-list needs to reuse internal structure from the joined-list.
Forking the joined-list marks all of its internal structure as shared, so that subsequent mutations to that structure
by the joining-list must copy first, thus not interfering with the joined-list.

But what if we don't care about the joined-list after joining? In that case, it would be nice if we could transfer
ownership of the joined-list's internal structure to the joining-list, so that it wouldn't have to copy before
subsequent mutations to that structure. To ensure integrity, the structure must no longer be accessible to the
joined-list (to enforce this, the joined-list could simply be cleared after the transfer).

To support this, I considered adding a method that would transfer nodes AND ownership to a new, identical list instance,
then clear the current instance. Let's call this method `extract()`. Inside `join()` we would now call `extract()`
instead of `fork()`, clearing the joined-list and obtaining a new list that owned all of the joined-list's former nodes
(if it had owned them). As `join()` would have exclusive access to this new list, it would not have to worry about
"stealing" and mangling the new list's internal structure.

The `extract()` method even had potential outside of `join()`, as a way for methods that were passed a list to
defensively "take ownership" of it, moving nodes to a new list that the caller could not mutate after the method exited.
This seemed like a useful primitive for joining in general: We could imagine a List implementation that just wants to
efficiently concatenate ArrayLists - to avoid full copying without compromising integrity, it would need a mechanism to
prevent callers from later mutating lists that had already been joined to it.

Unfortunately, the same indirection (having `join()` call `extract()`) that ensured integrity also felt like a potential
footgun:
- Even if it didn't violate the joining-list's integrity, it would likely be a misuse to access the joined-list after
  `join()`.
- With `join()` accepting a `Collection` argument, we'd either have to:
  1. Accept very inconsistent (and probably surprising) effects on arguments that implement `extract()` vs arguments that don't.
  2. Try to emulate `extract()` if the argument does not implement it - probably by clearing the collection after consuming it.
     This gets particularly questionable if the collection is a thread-safe implementation that may be receiving concurrent updates:
     Clearing it could drop elements that we did not consume.
  3. Change the signature of `join()` to accept a more specific argument type (see Q&A 3).
- `extract()` does not play nice if the joined-list _is_ the joining-list, or one is a sublist of the other.
  - 3 cases:
    1. <b>list-joins-list:</b> There is no effect. The list is extracted (cleared), then joined to a now-empty list, yielding the same elements.
    2. <b>list-joins-sublist:</b> The sublist is extracted (cleared), then joined to the remaining outer list.
    3. <b>sublist-joins-list:</b> Throws ConcurrentModificationException. The list is extracted (cleared), then the
       sublist detects that its parent list has been modified externally (modCount doesn't match expected), and throws.
  - In all cases, join-at-index becomes less intuitive:
    - Since `extract()` can affect the joining-list, it matters whether we interpret the index before or after `extract()` is called.
      - If after, the user may need to adjust their index to account for the region that would be removed by `extract()`.
      - If before, and index lies after the removed region, we would probably subtract the removed size from the index.
      - If before, and index lies in the removed region, we would probably update the index to the starting position of the removed region.
  - Note that if `join()` calls `fork()` instead of `extract()`, these problems go away, because the joining-list cannot
    be observably affected by creating or mutating a fork. Of course, we could just require that the user `fork()`
    _before_ calling `join()` if one list refers to the other - but this requirement would be easy to overlook.

With these concerns in mind, I decided to go with the safer option, and have `join()` call `fork()` on its argument.

</details>

---