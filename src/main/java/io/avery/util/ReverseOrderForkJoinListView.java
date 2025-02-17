package io.avery.util;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// Copied and adjusted from java.util.ReverseOrderListView
class ReverseOrderForkJoinListView<E> implements ForkJoinList<E> {
    final ForkJoinList<E> base;
    
    public static <T> ForkJoinList<T> of(ForkJoinList<T> list) {
        if (list instanceof ReverseOrderForkJoinListView<T> rolv) {
            return rolv.base;
        } else if (list instanceof RandomAccess) {
            return new ReverseOrderForkJoinListView.Rand<>(list);
        } else {
            return new ReverseOrderForkJoinListView<>(list);
        }
    }
    
    static class Rand<E> extends ReverseOrderForkJoinListView<E> implements RandomAccess {
        Rand(ForkJoinList<E> list) {
            super(list);
        }
    }
    
    private ReverseOrderForkJoinListView(ForkJoinList<E> list) {
        this.base = list;
    }
    
    class DescendingIterator implements Iterator<E> {
        final ListIterator<E> it = base.listIterator(base.size());
        public boolean hasNext() { return it.hasPrevious(); }
        public E next() { return it.previous(); }
        public void remove() {
            it.remove();
        }
    }
    
    class DescendingListIterator implements ListIterator<E> {
        final ListIterator<E> it;
        
        DescendingListIterator(int size, int pos) {
            if (pos < 0 || pos > size)
                throw new IndexOutOfBoundsException();
            it = base.listIterator(size - pos);
        }
        
        public boolean hasNext() {
            return it.hasPrevious();
        }
        
        public E next() {
            return it.previous();
        }
        
        public boolean hasPrevious() {
            return it.hasNext();
        }
        
        public E previous() {
            return it.next();
        }
        
        public int nextIndex() {
            return base.size() - it.nextIndex();
        }
        
        public int previousIndex() {
            return nextIndex() - 1;
        }
        
        public void remove() {
            it.remove();
        }
        
        public void set(E e) {
            it.set(e);
        }
        
        public void add(E e) {
            it.add(e);
            it.previous();
        }
    }
    
    // ========== Iterable ==========
    
    public void forEach(Consumer<? super E> action) {
        for (E e : this)
            action.accept(e);
    }
    
    public Iterator<E> iterator() {
        return new DescendingIterator();
    }
    
    public Spliterator<E> spliterator() {
        return Spliterators.spliterator(this, Spliterator.ORDERED);
    }
    
    // ========== Collection ==========
    
    public boolean add(E e) {
        base.add(0, e);
        return true;
    }
    
    public boolean addAll(Collection<? extends E> c) {
        @SuppressWarnings("unchecked")
        E[] adds = (E[]) c.toArray();
        if (adds.length == 0) {
            return false;
        } else {
            base.addAll(0, Arrays.asList(reverse(adds)));
            return true;
        }
    }
    
    public void clear() {
        base.clear();
    }
    
    public boolean contains(Object o) {
        return base.contains(o);
    }
    
    public boolean containsAll(Collection<?> c) {
        return base.containsAll(c);
    }
    
    // copied from AbstractList
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof List))
            return false;
        
        ListIterator<E> e1 = listIterator();
        ListIterator<?> e2 = ((List<?>) o).listIterator();
        while (e1.hasNext() && e2.hasNext()) {
            E o1 = e1.next();
            Object o2 = e2.next();
            if (!(o1==null ? o2==null : o1.equals(o2)))
                return false;
        }
        return !(e1.hasNext() || e2.hasNext());
    }
    
    // copied from AbstractList
    public int hashCode() {
        int hashCode = 1;
        for (E e : this)
            hashCode = 31*hashCode + (e==null ? 0 : e.hashCode());
        return hashCode;
    }
    
    public boolean isEmpty() {
        return base.isEmpty();
    }
    
    public Stream<E> parallelStream() {
        return StreamSupport.stream(spliterator(), true);
    }
    
    // copied from AbstractCollection
    public boolean remove(Object o) {
        Iterator<E> it = iterator();
        if (o==null) {
            while (it.hasNext()) {
                if (it.next()==null) {
                    it.remove();
                    return true;
                }
            }
        } else {
            while (it.hasNext()) {
                if (o.equals(it.next())) {
                    it.remove();
                    return true;
                }
            }
        }
        return false;
    }
    
    // copied from AbstractCollection
    public boolean removeAll(Collection<?> c) {
        Objects.requireNonNull(c);
        boolean modified = false;
        Iterator<?> it = iterator();
        while (it.hasNext()) {
            if (c.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }
    
    // copied from AbstractCollection
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        boolean modified = false;
        Iterator<E> it = iterator();
        while (it.hasNext()) {
            if (!c.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }
    
    public int size() {
        return base.size();
    }
    
    public Stream<E> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
    
    public Object[] toArray() {
        return reverse(base.toArray());
    }
    
    public <T> T[] toArray(T[] a) {
        return toArrayReversed(base, a);
    }
    
    public <T> T[] toArray(IntFunction<T[]> generator) {
        return reverse(base.toArray(generator));
    }
    
    // copied from AbstractCollection
    public String toString() {
        Iterator<E> it = iterator();
        if (! it.hasNext())
            return "[]";
        
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (;;) {
            E e = it.next();
            sb.append(e == this ? "(this Collection)" : e);
            if (! it.hasNext())
                return sb.append(']').toString();
            sb.append(',').append(' ');
        }
    }
    
    // ========== List ==========
    
    public void add(int index, E element) {
        int size = base.size();
        checkClosedRange(index, size);
        base.add(size - index, element);
    }
    
    public boolean addAll(int index, Collection<? extends E> c) {
        int size = base.size();
        checkClosedRange(index, size);
        @SuppressWarnings("unchecked")
        E[] adds = (E[]) c.toArray();
        if (adds.length == 0) {
            return false;
        } else {
            base.addAll(size - index, Arrays.asList(reverse(adds)));
            return true;
        }
    }
    
    public E get(int i) {
        int size = base.size();
        Objects.checkIndex(i, size);
        return base.get(size - i - 1);
    }
    
    public int indexOf(Object o) {
        int i = base.lastIndexOf(o);
        return i == -1 ? -1 : base.size() - i - 1;
    }
    
    public int lastIndexOf(Object o) {
        int i = base.indexOf(o);
        return i == -1 ? -1 : base.size() - i - 1;
    }
    
    public ListIterator<E> listIterator() {
        return new DescendingListIterator(base.size(), 0);
    }
    
    public ListIterator<E> listIterator(int index) {
        int size = base.size();
        checkClosedRange(index, size);
        return new DescendingListIterator(size, index);
    }
    
    public E remove(int index) {
        int size = base.size();
        Objects.checkIndex(index, size);
        return base.remove(size - index - 1);
    }
    
    public boolean removeIf(Predicate<? super E> filter) {
        return base.removeIf(filter);
    }
    
    public void replaceAll(UnaryOperator<E> operator) {
        base.replaceAll(operator);
    }
    
    public void sort(Comparator<? super E> c) {
        base.sort(Collections.reverseOrder(c));
    }
    
    public E set(int index, E element) {
        int size = base.size();
        Objects.checkIndex(index, size);
        return base.set(size - index - 1, element);
    }
    
    static void checkClosedRange(int index, int size) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }
    
    // ========== ForkJoinList ==========
    
    public void join(Collection<? extends E> other) {
        base.join(0, toCollectionReversed(other));
    }
    
    @Override
    public void join(int index, Collection<? extends E> other) {
        int size = base.size();
        checkClosedRange(index, size);
        base.join(size - index, toCollectionReversed(other));
    }
    
    public ForkJoinList<E> fork() {
        return new ReverseOrderForkJoinListView<>(base.fork());
    }
    
    public ForkJoinList<E> subList(int fromIndex, int toIndex) {
        int size = base.size();
        Objects.checkFromToIndex(fromIndex, toIndex, size);
        return new ReverseOrderForkJoinListView<>(base.subList(size - toIndex, size - fromIndex));
    }
    
    static <T> Collection<T> toCollectionReversed(Collection<T> collection) {
        if (collection instanceof SequencedCollection<T> sc) {
            return sc.reversed();
        }
        ForkJoinList<T> list = new TrieForkJoinList<>();
        list.join(collection);
        return list.reversed();
    }
    
    // Utils
    
    static <T> T[] reverse(T[] a) {
        int limit = a.length / 2;
        for (int i = 0, j = a.length - 1; i < limit; i++, j--) {
            T t = a[i];
            a[i] = a[j];
            a[j] = t;
        }
        return a;
    }
    
    static <T> T[] toArrayReversed(Collection<?> coll, T[] array) {
        T[] newArray = reverse(coll.toArray(Arrays.copyOfRange(array, 0, 0)));
        if (newArray.length > array.length) {
            return newArray;
        } else {
            System.arraycopy(newArray, 0, array, 0, newArray.length);
            if (array.length > newArray.length) {
                array[newArray.length] = null;
            }
            return array;
        }
    }
}
