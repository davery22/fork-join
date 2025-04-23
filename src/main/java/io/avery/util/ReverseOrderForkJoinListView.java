package io.avery.util;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

// A reversed() list implementation that mostly delegates to the JDK-internal reversed() list implementation,
// but tacks on necessary overrides for ForkJoinList methods.
class ReverseOrderForkJoinListView<E> implements ForkJoinList<E> {
    final ForkJoinList<E> list;
    final List<E> rlist;
    
    public static <T> ForkJoinList<T> of(ForkJoinList<T> list, List<T> rlist) {
        if (list instanceof ReverseOrderForkJoinListView<T> rolv) {
            return rolv.list;
        } else if (list instanceof RandomAccess) {
            return new ReverseOrderForkJoinListView.Rand<>(list, rlist);
        } else {
            return new ReverseOrderForkJoinListView<>(list, rlist);
        }
    }
    
    static class Rand<E> extends ReverseOrderForkJoinListView<E> implements RandomAccess {
        Rand(ForkJoinList<E> list, List<E> rlist) {
            super(list, rlist);
        }
    }
    
    private ReverseOrderForkJoinListView(ForkJoinList<E> list, List<E> rlist) {
        this.list = list;
        this.rlist = rlist;
    }
    
    public void add(int index, E element) {
        rlist.add(index, element);
    }
    
    public boolean add(E e) {
        return rlist.add(e);
    }
    
    public boolean addAll(int index, Collection<? extends E> c) {
        return rlist.addAll(index, c);
    }
    
    public boolean addAll(Collection<? extends E> c) {
        return rlist.addAll(c);
    }
    
    public void addFirst(E e) {
        rlist.addFirst(e);
    }
    
    public void addLast(E e) {
        rlist.addLast(e);
    }
    
    public void clear() {
        rlist.clear();
    }
    
    public boolean contains(Object o) {
        return rlist.contains(o);
    }
    
    public boolean containsAll(Collection<?> c) {
        return rlist.containsAll(c);
    }
    
    public boolean equals(Object o) {
        return rlist.equals(o);
    }
    
    public E get(int index) {
        return rlist.get(index);
    }
    
    public E getFirst() {
        return rlist.getFirst();
    }
    
    public E getLast() {
        return rlist.getLast();
    }
    
    public int hashCode() {
        return rlist.hashCode();
    }
    
    public int indexOf(Object o) {
        return rlist.indexOf(o);
    }
    
    public boolean isEmpty() {
        return rlist.isEmpty();
    }
    
    public Iterator<E> iterator() {
        return rlist.iterator();
    }
    
    public int lastIndexOf(Object o) {
        return rlist.lastIndexOf(o);
    }
    
    public ListIterator<E> listIterator() {
        return rlist.listIterator();
    }
    
    public ListIterator<E> listIterator(int index) {
        return rlist.listIterator(index);
    }
    
    public E remove(int index) {
        return rlist.remove(index);
    }
    
    public boolean remove(Object o) {
        return rlist.remove(o);
    }
    
    public boolean removeAll(Collection<?> c) {
        return rlist.removeAll(c);
    }
    
    public E removeFirst() {
        return rlist.removeFirst();
    }
    
    public E removeLast() {
        return rlist.removeLast();
    }
    
    public void replaceAll(UnaryOperator<E> operator) {
        rlist.replaceAll(operator);
    }
    
    public boolean retainAll(Collection<?> c) {
        return rlist.retainAll(c);
    }
    
    public E set(int index, E element) {
        return rlist.set(index, element);
    }
    
    public int size() {
        return rlist.size();
    }
    
    public void sort(Comparator<? super E> c) {
        rlist.sort(c);
    }
    
    public Spliterator<E> spliterator() {
        return rlist.spliterator();
    }
    
    public Object[] toArray() {
        return rlist.toArray();
    }
    
    public <T> T[] toArray(T[] a) {
        return rlist.toArray(a);
    }
    
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return rlist.toArray(generator);
    }
    
    public String toString() {
        return rlist.toString();
    }
    
    public Stream<E> parallelStream() {
        return rlist.parallelStream();
    }
    
    public Stream<E> stream() {
        return rlist.stream();
    }
    
    public boolean removeIf(Predicate<? super E> filter) {
        return rlist.removeIf(filter);
    }
    
    public void forEach(Consumer<? super E> action) {
        rlist.forEach(action);
    }
    
    // ========== ForkJoinList impl ==========
    
    public boolean join(Collection<? extends E> other) {
        return list.join(0, toCollectionReversed(other));
    }
    
    public boolean join(int index, Collection<? extends E> other) {
        int size = list.size();
        checkClosedRange(index, size);
        return list.join(size - index, toCollectionReversed(other));
    }
    
    public ForkJoinList<E> fork() {
        return list.fork().reversed();
    }
    
    public ForkJoinList<E> subList(int fromIndex, int toIndex) {
        int size = list.size();
        Objects.checkFromToIndex(fromIndex, toIndex, size);
        return list.subList(size - toIndex, size - fromIndex).reversed();
    }
    
    // ========== Utils ==========

    static void checkClosedRange(int index, int size) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

    static <T> T[] reverse(T[] a) {
        int limit = a.length / 2;
        for (int i = 0, j = a.length - 1; i < limit; i++, j--) {
            T t = a[i];
            a[i] = a[j];
            a[j] = t;
        }
        return a;
    }
    
    static <T> Collection<T> toCollectionReversed(Collection<T> collection) {
        if (collection instanceof SequencedCollection<T> sc) {
            return sc.reversed();
        }
        @SuppressWarnings("unchecked")
        T[] arr = (T[]) collection.toArray();
        return Arrays.asList(reverse(arr));
    }
}
