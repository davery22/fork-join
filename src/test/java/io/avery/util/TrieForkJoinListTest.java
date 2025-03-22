package io.avery.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
//        return Stream.concat(
//        Stream.of(
        return Stream.of(
            argumentSet("join1", id(TrieForkJoinListTest::join1)),
            argumentSet("join2", id(TrieForkJoinListTest::join2)),
            argumentSet("join3", id(TrieForkJoinListTest::join3)),
            argumentSet("join4", id(TrieForkJoinListTest::join4)),
            argumentSet("join5", id(TrieForkJoinListTest::join5)),
            argumentSet("subListFork1", id(TrieForkJoinListTest::subListFork1)),
            argumentSet("removeRange1", id(TrieForkJoinListTest::removeRange1)),
            argumentSet("removeRange2", id(TrieForkJoinListTest::removeRange2)),
            argumentSet("removeRange3", id(TrieForkJoinListTest::removeRange3)),
            argumentSet("removeRange4", id(TrieForkJoinListTest::removeRange4)),
            argumentSet("removeRange5", id(TrieForkJoinListTest::removeRange5)),
            argumentSet("removeRange6", id(TrieForkJoinListTest::removeRange6)),
            argumentSet("removeRange7", id(TrieForkJoinListTest::removeRange7)),
            argumentSet("removeRange8", id(TrieForkJoinListTest::removeRange8)),
            argumentSet("removeRange9", id(TrieForkJoinListTest::removeRange9)),
            argumentSet("removeRange10", id(TrieForkJoinListTest::removeRange10)),
            argumentSet("removeRange11", id(TrieForkJoinListTest::removeRange11)),
            argumentSet("removeRange12", id(TrieForkJoinListTest::removeRange12))
        );
//        ),
//        IntStream.rangeClosed(1, 100)
//            .mapToObj(i -> argumentSet("fuzz" + i, id(factory -> fuzz(i, factory))))
//        );
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
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        list.join(list);
        return list;
    }
    
    static ForkJoinList<Integer> join3(Factory factory) {
        // Prepend once
        ForkJoinList<Integer> right = listOfSize(factory, 80000);
        ForkJoinList<Integer> left = factory.get();
        left.add(5);
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join4(Factory factory) {
        // Prepend several times
        ForkJoinList<Integer> right = listOfSize(factory, 80000);
        ForkJoinList<Integer> left = factory.get();
        left.add(5);
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
        ForkJoinList<Integer> left = listOfSize(factory, leftSize);
        ForkJoinList<Integer> right = listOfSize(factory, rightSize);
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < SPAN*SPAN; i++) {
            result.join(left);
            result.join(right);
        }
        return result;
    }
    
    static ForkJoinList<Integer> subListFork1(Factory factory) {
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        return list.subList(100, 9900).fork();
    }
    
    static ForkJoinList<Integer> removeRange1(Factory factory) {
        // Remove range that starts and ends in the root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(100, 9900).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange2(Factory factory) {
        // Remove everything
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(0, 10000).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange3(Factory factory) {
        // Remove nothing (front)
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(0, 0).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange4(Factory factory) {
        // Remove nothing (back)
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(10000, 10000).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange5(Factory factory) {
        // Remove prefix ending at tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(0, size - SPAN).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange6(Factory factory) {
        // Remove prefix ending in tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(0, size-1).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange7(Factory factory) {
        // Remove prefix ending in root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(0, 9000).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange8(Factory factory) {
        // Remove suffix starting at tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(size - SPAN, size).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange9(Factory factory) {
        // Remove suffix starting in tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(size-1, size).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange10(Factory factory) {
        // Remove suffix starting in root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(9000, 10000).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange11(Factory factory) {
        // Remove range that starts and ends in the tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(size-SPAN, size-SPAN/2).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange12(Factory factory) {
        // Remove range that starts in the root and ends in the tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(100, size-SPAN/2).clear();
        return list;
    }
    
    static ForkJoinList<Integer> fuzz(int seed, Factory factory) {
        Random random = new Random(seed);
        ForkJoinList<Integer> list = factory.get();
        int len = random.nextInt(5, 8675309);
        for (int i = 0; i < len; i++) {
            list.add(i);
        }
        int splits = random.nextInt(1, 100);
        ForkJoinList<Integer> result = null;
        for (int i = 0; i < splits; i++) {
            int end = random.nextInt(0, len+1);
            int start = random.nextInt(0, end+1);
            var sublist = list.subList(start, end).fork();
            if (result == null) {
                result = sublist;
            }
            else {
                result.join(sublist);
            }
        }
        return result;
    }
    
    static ForkJoinList<Integer> listOfSize(Factory factory, int size) {
        ForkJoinList<Integer> list = factory.get();
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
        return list;
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