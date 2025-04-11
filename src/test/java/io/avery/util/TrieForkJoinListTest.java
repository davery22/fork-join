package io.avery.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class TrieForkJoinListTest {
    static final int SPAN = TrieForkJoinList.SPAN;
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
            argumentSet("joinAtIndex1", id(TrieForkJoinListTest::joinAtIndex1)),
            argumentSet("joinAtIndex2", id(TrieForkJoinListTest::joinAtIndex2)),
            argumentSet("joinAtIndex3", id(TrieForkJoinListTest::joinAtIndex3)),
            argumentSet("joinAtIndex4", id(TrieForkJoinListTest::joinAtIndex4)),
            argumentSet("joinAtIndex5", id(TrieForkJoinListTest::joinAtIndex5)),
            argumentSet("joinAtIndex6", id(TrieForkJoinListTest::joinAtIndex6)),
            argumentSet("joinAtIndex7", id(TrieForkJoinListTest::joinAtIndex7)),
            argumentSet("joinAtIndex8", id(TrieForkJoinListTest::joinAtIndex8)),
            argumentSet("addAll1", id(TrieForkJoinListTest::addAll1)),
            argumentSet("addAll2", id(TrieForkJoinListTest::addAll2)),
            argumentSet("addAll3", id(TrieForkJoinListTest::addAll3)),
            argumentSet("addAll4", id(TrieForkJoinListTest::addAll4)),
            argumentSet("addAllAtIndex1", id(TrieForkJoinListTest::addAllAtIndex1)),
            argumentSet("addAllAtIndex2", id(TrieForkJoinListTest::addAllAtIndex2)),
            argumentSet("addAllAtIndex3", id(TrieForkJoinListTest::addAllAtIndex3)),
            argumentSet("addAllAtIndex4", id(TrieForkJoinListTest::addAllAtIndex4)),
            argumentSet("addAllAtIndex5", id(TrieForkJoinListTest::addAllAtIndex5)),
            argumentSet("addAllAtIndex6", id(TrieForkJoinListTest::addAllAtIndex6)),
            argumentSet("addAllAtIndex7", id(TrieForkJoinListTest::addAllAtIndex7)),
            argumentSet("addAllAtIndex8", id(TrieForkJoinListTest::addAllAtIndex8)),
            argumentSet("addAllAtIndex9", id(TrieForkJoinListTest::addAllAtIndex9)),
            argumentSet("addAllAtIndex10", id(TrieForkJoinListTest::addAllAtIndex10)),
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
            argumentSet("removeRange12", id(TrieForkJoinListTest::removeRange12)),
            argumentSet("toArray1", id(TrieForkJoinListTest::toArray1)),
            argumentSet("toArray2", id(TrieForkJoinListTest::toArray2)),
            argumentSet("toArray3", id(TrieForkJoinListTest::toArray3)),
            argumentSet("toArray4", id(TrieForkJoinListTest::toArray4)),
            argumentSet("toArray5", id(TrieForkJoinListTest::toArray5)),
            argumentSet("toArray6", id(TrieForkJoinListTest::toArray6)),
            argumentSet("subListToArray1", id(TrieForkJoinListTest::subListToArray1)),
            argumentSet("subListToArray2", id(TrieForkJoinListTest::subListToArray2)),
            argumentSet("iterator1", id(TrieForkJoinListTest::iterator1)),
            argumentSet("iterator2", id(TrieForkJoinListTest::iterator2)),
            argumentSet("iterator3", id(TrieForkJoinListTest::iterator3)),
            argumentSet("iterator4", id(TrieForkJoinListTest::iterator4)),
            argumentSet("iterator5", id(TrieForkJoinListTest::iterator5)),
            argumentSet("iterator6", id(TrieForkJoinListTest::iterator6)),
            argumentSet("iterator7", id(TrieForkJoinListTest::iterator7)),
            argumentSet("iterator8", id(TrieForkJoinListTest::iterator8)),
            argumentSet("listIterator1", id(TrieForkJoinListTest::listIterator1))
        );
//        ),
//        IntStream.rangeClosed(1, 100)
//            .mapToObj(i -> argumentSet("fuzz" + i, id(factory -> fuzz(i, factory))))
//        );
    }
    
    @ParameterizedTest
    @MethodSource("provideCreators")
    void testCreatesEqual(Function<Factory, Object> creator) {
        Object expected = creator.apply(ARRAY_FJL);
        Object actual = creator.apply(TRIE_FJL);
        if (expected instanceof Object[] e && actual instanceof Object[] a) {
            assertArrayEquals(e, a);
        }
        else {
            assertEquals(expected, actual);
        }
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
    
    static ForkJoinList<Integer> joinAtIndex1(Factory factory) {
        // Insert a tail into the existing tail, new tail fits into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> toInsert = listOfSize(factory, SPAN/2);
        list.join(size - SPAN/4, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex2(Factory factory) {
        // Insert a tail into the existing tail, new tail does not fit into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> toInsert = listOfSize(factory, SPAN);
        list.join(size - SPAN/4, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex3(Factory factory) {
        // Insert a tree into the existing tail, new tail fits into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> toInsert = listOfSize(factory, size);
        list.join(size - SPAN/4, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex4(Factory factory) {
        // Insert a tree into the existing tail, new tail does not fit into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> toInsert = listOfSize(factory, size + SPAN/2);
        list.join(size - SPAN/4, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex5(Factory factory) {
        // Insert a tree at start of the existing tail, new tail fits into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> toInsert = listOfSize(factory, size);
        list.join(size - SPAN/2, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex6(Factory factory) {
        // Insert a tree at start of the existing tail, new tail does not fit into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> toInsert = listOfSize(factory, size + SPAN/2);
        list.join(size - SPAN/2, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex7(Factory factory) {
        // Prepend a tree
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> toInsert = listOfSize(factory, 5000);
        list.join(0, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex8(Factory factory) {
        // Insert a tree into root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> toInsert = listOfSize(factory, 5000);
        list.join(4200, toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> addAll1(Factory factory) {
        // Add empty list
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        List<Integer> toAdd = List.of();
        list.addAll(toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAll2(Factory factory) {
        // Add list that fits in tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toAdd = IntStream.range(0, SPAN/2).boxed().toList();
        list.addAll(toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAll3(Factory factory) {
        // Add large list to full tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toAdd = IntStream.range(0, 1000).boxed().toList();
        list.addAll(toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAll4(Factory factory) {
        // Add large list to non-full tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toAdd = IntStream.range(0, 1000).boxed().toList();
        list.addAll(toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex1(Factory factory) {
        // Add at end
        ForkJoinList<Integer> list = listOfSize(factory, 500);
        List<Integer> toAdd = IntStream.range(0, 5000).boxed().toList();
        list.addAll(list.size(), toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex2(Factory factory) {
        // Add empty
        ForkJoinList<Integer> list = listOfSize(factory, 500);
        List<Integer> toAdd = List.of();
        list.addAll(list.size()/2, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex3(Factory factory) {
        // Insert elements into the existing tail, elements fit into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toAdd = IntStream.range(0, SPAN/2).boxed().toList();
        list.addAll(size - SPAN/4, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex4(Factory factory) {
        // Insert few elements into the existing tail, elements do not fit into existing tail
        int size = SPAN*1000 - SPAN/2;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toAdd = IntStream.range(0, SPAN).boxed().toList();
        list.addAll(size - SPAN/4, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex5(Factory factory) {
        // Insert few elements into the existing tail, elements do not fit into existing tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toAdd = IntStream.range(0, SPAN + SPAN/2).boxed().toList();
        list.addAll(size - SPAN/2, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex6(Factory factory) {
        // Insert many elements into the existing tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size - SPAN/2);
        List<Integer> toAdd = IntStream.range(0, size + 3*SPAN/4).boxed().toList();
        list.addAll(size - 3*SPAN/4, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex7(Factory factory) {
        // Insert many elements into the existing tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toAdd = IntStream.range(0, size + SPAN/2).boxed().toList();
        list.addAll(size - 3*SPAN/4, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex8(Factory factory) {
        // Prepend few elements
        ForkJoinList<Integer> list = listOfSize(factory, 500);
        List<Integer> toAdd = IntStream.range(0, SPAN/2).boxed().toList();
        list.addAll(0, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex9(Factory factory) {
        // Prepend many elements
        ForkJoinList<Integer> list = listOfSize(factory, 500);
        List<Integer> toAdd = IntStream.range(0, 1000).boxed().toList();
        list.addAll(0, toAdd);
        return list;
    }
    
    static ForkJoinList<Integer> addAllAtIndex10(Factory factory) {
        // Insert into root
        ForkJoinList<Integer> list = listOfSize(factory, 500);
        List<Integer> toAdd = IntStream.range(0, 1000).boxed().toList();
        list.addAll(200, toAdd);
        return list;
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
    
    static Object toArray1(Factory factory) {
        // Tail-only
        return listOfSize(factory, SPAN/2).toArray();
    }
    
    static Object toArray2(Factory factory) {
        // Shallow root and tail
        return listOfSize(factory, SPAN + SPAN/2).toArray();
    }
    
    static Object toArray3(Factory factory) {
        // Deep root and tail
        return listOfSize(factory, SPAN*1000).toArray();
    }
    
    static Object toArray4(Factory factory) {
        // Pass in undersized array
        return listOfSize(factory, SPAN*10).toArray(new Object[0]);
    }
    
    static Object toArray5(Factory factory) {
        // Pass in oversized array
        return listOfSize(factory, SPAN*10).toArray(new Object[SPAN*100]);
    }
    
    static Object toArray6(Factory factory) {
        // Concatenated list
        ForkJoinList<Integer> list = listOfSize(factory, 113);
        list.join(list.fork());
        return list.toArray();
    }
    
    static Object subListToArray1(Factory factory) {
        // Range in shallow root
        return listOfSize(factory, SPAN*2).subList(SPAN/4, SPAN/2).toArray();
    }
    
    static Object subListToArray2(Factory factory) {
        // Range in deep root
        return listOfSize(factory, SPAN*1000).subList(100, 1000).toArray();
    }
    
    static void iterSetForward(ListIterator<Integer> iter, List<Integer> result) {
        while (iter.hasNext()) {
            int n;
            result.add(iter.nextIndex());
            result.add(n = iter.next());
            iter.set(n+1);
        }
    }
    
    static void iterSetBackward(ListIterator<Integer> iter, List<Integer> result) {
        while (iter.hasPrevious()) {
            int n;
            result.add(iter.previousIndex());
            result.add(n = iter.previous());
            iter.set(n+1);
        }
    }
    
    static ForkJoinList<Integer> iterator1(Factory factory) {
        // Iterate front-to-back then back-to-front, twice
        ForkJoinList<Integer> list = listOfSize(factory, 10000).fork();
        ListIterator<Integer> iter = list.listIterator();
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetForward(iter, result);
            iterSetBackward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> iterator2(Factory factory) {
        // Iterate back-to-front then front-to-back, twice
        ForkJoinList<Integer> list = listOfSize(factory, 10000).fork();
        ListIterator<Integer> iter = list.listIterator(list.size());
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetBackward(iter, result);
            iterSetForward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> iterator3(Factory factory) {
        // Iterate front-to-back then back-to-front, twice (starting at tailOffset)
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size).fork();
        ListIterator<Integer> iter = list.listIterator(size-SPAN);
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetForward(iter, result);
            iterSetBackward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> iterator4(Factory factory) {
        // Iterate back-to-front then front-to-back, twice (starting at tailOffset)
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size).fork();
        ListIterator<Integer> iter = list.listIterator(size-SPAN);
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetBackward(iter, result);
            iterSetForward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> iterator5(Factory factory) {
        // Iterate front-to-back then back-to-front, twice (rootShift = 0, non-full root)
        ForkJoinList<Integer> list = listOfSize(factory, SPAN + SPAN);
        list.subList(SPAN/2, SPAN).clear();
        list = list.fork();
        ListIterator<Integer> iter = list.listIterator();
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetForward(iter, result);
            iterSetBackward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> iterator6(Factory factory) {
        // Iterate back-to-front then front-to-back, twice (rootShift = 0, non-full root)
        ForkJoinList<Integer> list = listOfSize(factory, SPAN + SPAN);
        list.subList(SPAN/2, SPAN).clear();
        list = list.fork();
        ListIterator<Integer> iter = list.listIterator(list.size());
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetBackward(iter, result);
            iterSetForward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> iterator7(Factory factory) {
        // Iterate front-to-back then back-to-front, twice (rootShift = 0, non-full root, starting at tailOffset)
        ForkJoinList<Integer> list = listOfSize(factory, SPAN + SPAN);
        list.subList(SPAN/2, SPAN).clear();
        list = list.fork();
        ListIterator<Integer> iter = list.listIterator(SPAN/2);
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetForward(iter, result);
            iterSetBackward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> iterator8(Factory factory) {
        // Iterate back-to-front then front-to-back, twice (rootShift = 0, non-full root, starting at tailOffset)
        ForkJoinList<Integer> list = listOfSize(factory, SPAN + SPAN);
        list.subList(SPAN/2, SPAN).clear();
        list = list.fork();
        ListIterator<Integer> iter = list.listIterator(SPAN/2);
        ForkJoinList<Integer> result = factory.get();
        for (int i = 0; i < 2; i++) {
            iterSetBackward(iter, result);
            iterSetForward(iter, result);
        }
        return result;
    }
    
    static ForkJoinList<Integer> listIterator1(Factory factory) {
        // Add all elements via iterator
        ForkJoinList<Integer> list = factory.get();
        ListIterator<Integer> iter = list.listIterator();
        // TODO: ArrayList is 10-20x faster. Optimize
//        var start = Instant.now();
        int i;
        for (i = 0; i < 1000; i++) {
            iter.add(i);
        }
        for (;;) {
            if (!iter.hasPrevious()) { break; }
            iter.previous();
            iter.add(i++);
            if (!iter.hasPrevious()) { break; }
            iter.previous();
        }
        for (;;) {
            if (!iter.hasNext()) { break; }
            iter.next();
            iter.add(i++);
        }
//        var end = Instant.now();
//        System.out.println(Duration.between(start, end));
        return list;
    }
    
    @Test
    void testIteratorInvalidRemove() {
        Iterator<Object> iter = new TrieForkJoinList<>().iterator();
        assertThrows(IllegalStateException.class, iter::remove);
    }
    
    @Test
    void testInitFromCollection1() {
        // Init from big collection
        List<Integer> expected = IntStream.range(0, 1_000_000).boxed().toList();
        List<Integer> actual = new TrieForkJoinList<>(expected);
        assertEquals(expected, actual);
    }
    
    @Test
    void testInitFromCollection2() {
        // Init from small collection
        List<Integer> expected = IntStream.range(0, SPAN/2).boxed().toList();
        List<Integer> actual = new TrieForkJoinList<>(expected);
        assertEquals(expected, actual);
    }
    
    @Test
    void testInitFromCollection3() {
        // Init from empty collection
        List<Integer> expected = List.of();
        List<Integer> actual = new TrieForkJoinList<>(expected);
        assertEquals(expected, actual);
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