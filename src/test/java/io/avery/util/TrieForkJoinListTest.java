package io.avery.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class TrieForkJoinListTest {
    static final int SPAN = 16; // TODO: = TrieForkJoinList.SPAN
    static final Factory TRIE_FJL = TrieForkJoinList::new;
    static final Factory ARRAY_FJL = ArrayForkJoinList::new;
    
    interface Factory {
        <E> ForkJoinList<E> get();
    }
    
    static Function<Factory, Object> id(Function<Factory, Object> o) {
        return o;
    }
    
    static Stream<ArgumentSet> provideCreators() {
        return Stream.of(
            argumentSet("join1", id(TrieForkJoinListTest::join1)),
            argumentSet("join2", id(TrieForkJoinListTest::join2)),
            argumentSet("join3", id(TrieForkJoinListTest::join3)),
            argumentSet("join4", id(TrieForkJoinListTest::join4)),
            argumentSet("join5", id(TrieForkJoinListTest::join5))
        );
    }
    
    @ParameterizedTest
    @MethodSource("provideCreators")
    void testCreatesEqual(Function<Factory, Object> creator) {
        assertEquals(creator.apply(ARRAY_FJL), creator.apply(TRIE_FJL));
    }
    
    static ForkJoinList<Integer> join1(Factory factory) {
        // Same-sized join
        ForkJoinList<Integer> left = factory.get();
        ForkJoinList<Integer> right = factory.get();
        for (int i = 0; i < 1000; i++) {
            left.add(i);
            right.add(2000-i);
        }
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join2(Factory factory) {
        // Self-join
        ForkJoinList<Integer> list = factory.get();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        list.join(list);
        return list;
    }
    
    static ForkJoinList<Integer> join3(Factory factory) {
        // Prepend once
        ForkJoinList<Integer> left = factory.get();
        ForkJoinList<Integer> right = factory.get();
        left.add(5);
        for (int i = 0; i < 80000; i++) {
            right.add(i);
        }
        left.join(right);
        left.toString();
        return left;
    }
    
    static ForkJoinList<Integer> join4(Factory factory) {
        // Prepend several times
        ForkJoinList<Integer> left = factory.get();
        ForkJoinList<Integer> right = factory.get();
        left.add(5);
        for (int i = 0; i < 80000; i++) {
            right.add(i);
        }
        var result = left;
        for (int i = 0; i < 100; i++) {
            result = left.fork();
            result.join(right);
            right = result;
        }
        return result;
    }
    
    static ForkJoinList<Integer> join5(Factory factory) {
        // Concatenate in a pattern such that leftLen < SPAN but leftLen + rightLen > SPAN
        int leftSize  = SPAN*2;    // Tree would have 2 levels after tail push-down, with 2 children in root
        int rightSize = SPAN*SPAN; // Tree would have 2 fully-dense levels after tail push-down
        ForkJoinList<Integer> left = factory.get();
        ForkJoinList<Integer> right = factory.get();
        for (int i = 0; i < leftSize; i++) {
            left.add(i);
        }
        for (int i = 0; i < rightSize; i++) {
            right.add(i);
        }
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < SPAN*SPAN; i++) {
            result.join(left);
            result.join(right);
        }
        return result;
    }
    
    static class ArrayForkJoinList<E> extends AbstractList<E> implements ForkJoinList<E> {
        private final List<E> base;
        
        public ArrayForkJoinList() {
            this.base = new ArrayList<>();
        }
        
        private ArrayForkJoinList(List<E> base) {
            this.base = base;
        }
        
        @Override
        public boolean join(Collection<? extends E> other) {
            return base.addAll(other);
        }
        
        @Override
        public boolean join(int index, Collection<? extends E> other) {
            return base.addAll(index, other);
        }
        
        @Override
        public ForkJoinList<E> fork() {
            return new ArrayForkJoinList<>(new ArrayList<>(base));
        }
        
        @Override
        public int size() {
            return base.size();
        }
        
        @Override
        public boolean isEmpty() {
            return base.isEmpty();
        }
        
        @Override
        public boolean contains(Object o) {
            return base.contains(o);
        }
        
        @Override
        public Iterator<E> iterator() {
            return base.iterator();
        }
        
        @Override
        public Object[] toArray() {
            return base.toArray();
        }
        
        @Override
        public <T> T[] toArray(T[] a) {
            return base.toArray(a);
        }
        
        @Override
        public boolean add(E e) {
            return base.add(e);
        }
        
        @Override
        public boolean remove(Object o) {
            return base.remove(o);
        }
        
        @Override
        public boolean containsAll(Collection<?> c) {
            return base.containsAll(c);
        }
        
        @Override
        public boolean addAll(Collection<? extends E> c) {
            return base.addAll(c);
        }
        
        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
            return base.addAll(index, c);
        }
        
        @Override
        public boolean removeAll(Collection<?> c) {
            return base.removeAll(c);
        }
        
        @Override
        public boolean retainAll(Collection<?> c) {
            return base.retainAll(c);
        }
        
        @Override
        public void clear() {
            base.clear();
        }
        
        @Override
        public E get(int index) {
            return base.get(index);
        }
        
        @Override
        public E set(int index, E element) {
            return base.set(index, element);
        }
        
        @Override
        public void add(int index, E element) {
            base.add(index, element);
        }
        
        @Override
        public E remove(int index) {
            return base.remove(index);
        }
        
        @Override
        public int indexOf(Object o) {
            return base.indexOf(o);
        }
        
        @Override
        public int lastIndexOf(Object o) {
            return base.lastIndexOf(o);
        }
        
        @Override
        public ListIterator<E> listIterator() {
            return base.listIterator();
        }
        
        @Override
        public ListIterator<E> listIterator(int index) {
            return base.listIterator(index);
        }
        
        @Override
        public ForkJoinList<E> subList(int fromIndex, int toIndex) {
            return new ArrayForkJoinList<>(base.subList(fromIndex, toIndex));
        }
    }
}