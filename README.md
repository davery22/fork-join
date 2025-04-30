# Fork-Join Data Structures

Data structures with cheap copy ("fork") and merge ("join") operations. These operations are so-named in reference to
the JDK's own fork-join concurrency framework that underpins parallel streams, which these data structures were intended
to integrate with.

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
them is that the `join()` methods will `fork()` the argument passed to them if it happens to be a `ForkJoinList` (and
then use a sublinear algorithm to integrate the result, if possible).

Since `ForkJoinList` extends `List`, a `TrieForkJoinList` can be used as a drop-in replacement implementation for any
`List`, and it will "just work"*.

*Almost. Due to the nature of structural sharing, in certain situations, `set()` has to be considered a "structural
modification". This means that an iterator will throw `ConcurrentModificationException` if we attempt to reuse it after
calling `set()` on another iterator, or the list. These "certain situations" won't occur if we never use `fork()` or
`join()` - but then, what's the point?

### API Notes

TODO

## Implementation Details

TrieForkJoinList is an implementation of a Relaxed Radix Balanced (RRB) Tree, drawing primarily from the
[technical report](https://infoscience.epfl.ch/server/api/core/bitstreams/e5d662ea-1e8d-4dda-b917-8cbb8bb40bf9/content)
by Bagwell and Rompf, and subsequent [master's thesis](https://hypirion.com/thesis.pdf) by L'orange. RRB trees extend
the better-known "persistent vector" (radix balanced tree) with O(logN) concatenation and split operations, mainly by
"relaxing" the density requirement of the tree (allowing interior nodes to be non-full). This introduces a notion of
"balance", addressing how node children should be redistributed during operations like concatenation to trade off speed
of the operation vs node density (which affects speed of subsequent operations). Sparse subtrees track the cumulative
sizes of their children, to support "relaxed" radix indexing.

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

Other notable features of TrieForkJoinList:
1. Rather than forcing update operations to return a new instance (copy + mutate, as in "persistent" data
   structures), we separate a copy operation (fork) from mutative operations, allowing the implementation to
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
3. We do not use a planning phase during concatenation. This can lead to redundant copying in some
   cases, but does not appear to be a bottleneck. In general, we try to postpone copying in order to fuse with
   other operations, within "reason" (depending on your tolerance for code sprawl / complexity).
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
