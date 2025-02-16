package io.avery.util;

import java.util.Collection;
import java.util.List;

public interface ForkJoinList<E> extends List<E> {
    ForkJoinList<E> fork();
    void join(Collection<? extends E> other);
    ForkJoinList<E> splice();
    ForkJoinList<E> splice(Collection<? extends E> replacement);
    ForkJoinList<E> subList(int fromIndex, int toIndex);
    
    default ForkJoinList<E> reversed() {
        return ReverseOrderForkJoinListView.of(this);
    }
}
