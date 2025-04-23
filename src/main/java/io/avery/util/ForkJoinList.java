package io.avery.util;

import java.util.Collection;
import java.util.List;

/**
 * A {@code List} that supports potentially sublinear copy ({@code fork}) and bulk insertion/concatenation
 * ({@code join}) methods.
 *
 * <p>A {@code subList} or {@code reversed} view of a {@code ForkJoinList} is itself a {@code ForkJoinList}, though it
 * may not retain the same performance characteristics as the original list.
 *
 * @param <E> the type of elements in this list
 */
public interface ForkJoinList<E> extends List<E> {
    /**
     * Like {@link #addAll(Collection)}, but attempts to {@link #fork} its argument before appending if it is a
     * {@code ForkJoinList}, and may use a sublinear algorithm to adjoin the resultant copy if possible.
     *
     * @param c collection containing elements to be added to this list
     * @return {@code true} if this list changed as a result of the call
     */
    boolean join(Collection<? extends E> c);
    
    /**
     * Like {@link #addAll(int, Collection)}, but attempts to {@link #fork} its argument before inserting if it is a
     * {@code ForkJoinList}, and may use a sublinear algorithm to adjoin the resultant copy if possible.
     *
     * @param index index at which to insert the first element from the specified collection
     * @param c collection containing elements to be added to this list
     * @return {@code true} if this list changed as a result of the call
     */
    boolean join(int index, Collection<? extends E> c);
    
    /**
     * Returns a shallow copy of this {@code ForkJoinList} instance. (The elements themselves are not copied.)
     *
     * @implSpec Sublinear implementations will likely use structural sharing to avoid visiting each element when
     * copying. This must not impact the results of subsequent operations on this list, but may impact the performance
     * of subsequent modifications to this list.
     *
     * @return a copy of this list
     */
    ForkJoinList<E> fork();
    
    ForkJoinList<E> subList(int fromIndex, int toIndex);
    
    default ForkJoinList<E> reversed() {
        return ReverseOrderForkJoinListView.of(this, List.super.reversed());
    }
}
