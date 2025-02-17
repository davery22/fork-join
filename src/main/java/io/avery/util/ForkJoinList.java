package io.avery.util;

import java.util.Collection;
import java.util.List;

public interface ForkJoinList<E> extends List<E> {
    void join(Collection<? extends E> other); // TODO: For concurrent collections, clear() may lose unaccounted-for elements
    void join(int index, Collection<? extends E> other);
    ForkJoinList<E> fork();
    ForkJoinList<E> subList(int fromIndex, int toIndex);
    
    default ForkJoinList<E> reversed() {
        return ReverseOrderForkJoinListView.of(this);
    }
}
