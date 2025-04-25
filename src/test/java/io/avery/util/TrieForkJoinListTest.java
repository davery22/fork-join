package io.avery.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
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
            argumentSet("hashCode1", id(TrieForkJoinListTest::hashCode1)),
            argumentSet("hashCode2", id(TrieForkJoinListTest::hashCode2)),
            argumentSet("set1", id(TrieForkJoinListTest::getSet1)),
            argumentSet("subListSet1", id(TrieForkJoinListTest::subListSet1)),
            argumentSet("join1", id(TrieForkJoinListTest::join1)),
            argumentSet("join2", id(TrieForkJoinListTest::join2)),
            argumentSet("join3", id(TrieForkJoinListTest::join3)),
            argumentSet("join4", id(TrieForkJoinListTest::join4)),
            argumentSet("join5", id(TrieForkJoinListTest::join5)),
            argumentSet("join6", id(TrieForkJoinListTest::join6)),
            argumentSet("join7", id(TrieForkJoinListTest::join7)),
            argumentSet("join8", id(TrieForkJoinListTest::join8)),
            argumentSet("join9", id(TrieForkJoinListTest::join9)),
            argumentSet("join10", id(TrieForkJoinListTest::join10)),
            argumentSet("join11", id(TrieForkJoinListTest::join11)),
            argumentSet("join12", id(TrieForkJoinListTest::join12)),
            argumentSet("join13", id(TrieForkJoinListTest::join13)),
            argumentSet("joinAtIndex1", id(TrieForkJoinListTest::joinAtIndex1)),
            argumentSet("joinAtIndex2", id(TrieForkJoinListTest::joinAtIndex2)),
            argumentSet("joinAtIndex3", id(TrieForkJoinListTest::joinAtIndex3)),
            argumentSet("joinAtIndex4", id(TrieForkJoinListTest::joinAtIndex4)),
            argumentSet("joinAtIndex5", id(TrieForkJoinListTest::joinAtIndex5)),
            argumentSet("joinAtIndex6", id(TrieForkJoinListTest::joinAtIndex6)),
            argumentSet("joinAtIndex7", id(TrieForkJoinListTest::joinAtIndex7)),
            argumentSet("joinAtIndex8", id(TrieForkJoinListTest::joinAtIndex8)),
            argumentSet("joinAtIndex9", id(TrieForkJoinListTest::joinAtIndex9)),
            argumentSet("joinAtIndex10", id(TrieForkJoinListTest::joinAtIndex10)),
            argumentSet("joinAtIndex11", id(TrieForkJoinListTest::joinAtIndex11)),
            argumentSet("joinAtIndex12", id(TrieForkJoinListTest::joinAtIndex12)),
            argumentSet("joinAtIndex13", id(TrieForkJoinListTest::joinAtIndex13)),
            argumentSet("add1", id(TrieForkJoinListTest::add1)),
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
            argumentSet("subListFork2", id(TrieForkJoinListTest::subListFork2)),
            argumentSet("subListFork3", id(TrieForkJoinListTest::subListFork3)),
            argumentSet("subListFork4", id(TrieForkJoinListTest::subListFork4)),
            argumentSet("subListFork5", id(TrieForkJoinListTest::subListFork5)),
            argumentSet("subListFork6", id(TrieForkJoinListTest::subListFork6)),
            argumentSet("subListFork7", id(TrieForkJoinListTest::subListFork7)),
            argumentSet("subListFork8", id(TrieForkJoinListTest::subListFork8)),
            argumentSet("subListFork9", id(TrieForkJoinListTest::subListFork9)),
            argumentSet("subListFork10", id(TrieForkJoinListTest::subListFork10)),
            argumentSet("subListFork11", id(TrieForkJoinListTest::subListFork11)),
            argumentSet("subListFork12", id(TrieForkJoinListTest::subListFork12)),
            argumentSet("subListFork13", id(TrieForkJoinListTest::subListFork13)),
            argumentSet("subListFork14", id(TrieForkJoinListTest::subListFork14)),
            argumentSet("subListFork15", id(TrieForkJoinListTest::subListFork15)),
            argumentSet("subListFork16", id(TrieForkJoinListTest::subListFork16)),
            argumentSet("removeAtIndex1", id(TrieForkJoinListTest::removeAtIndex1)),
            argumentSet("removeAtIndex2", id(TrieForkJoinListTest::removeAtIndex2)),
            argumentSet("removeAtIndex3", id(TrieForkJoinListTest::removeAtIndex3)),
            argumentSet("removeAtIndex4", id(TrieForkJoinListTest::removeAtIndex4)),
            argumentSet("removeAtIndex5", id(TrieForkJoinListTest::removeAtIndex5)),
            argumentSet("removeAtIndex6", id(TrieForkJoinListTest::removeAtIndex6)),
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
            argumentSet("removeRange13", id(TrieForkJoinListTest::removeRange13)),
            argumentSet("removeIf1", id(TrieForkJoinListTest::removeIf1)),
            argumentSet("removeIf2", id(TrieForkJoinListTest::removeIf2)),
            argumentSet("removeAll1", id(TrieForkJoinListTest::removeAll1)),
            argumentSet("removeAll2", id(TrieForkJoinListTest::removeAll2)),
            argumentSet("retainAll1", id(TrieForkJoinListTest::retainAll1)),
            argumentSet("retainAll2", id(TrieForkJoinListTest::retainAll2)),
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
            argumentSet("subListIterator1", id(TrieForkJoinListTest::subListIterator1)),
            argumentSet("subListIterator2", id(TrieForkJoinListTest::subListIterator2)),
            argumentSet("subListIterator3", id(TrieForkJoinListTest::subListIterator3)),
            argumentSet("listIterator1", id(TrieForkJoinListTest::listIterator1)),
            argumentSet("listIterator2", id(TrieForkJoinListTest::listIterator2)),
            argumentSet("listIterator3", id(TrieForkJoinListTest::listIterator3)),
            argumentSet("listIterator4", id(TrieForkJoinListTest::listIterator4)),
            argumentSet("listIterator5", id(TrieForkJoinListTest::listIterator5)),
            argumentSet("subListListIterator1", id(TrieForkJoinListTest::subListListIterator1)),
            argumentSet("subListListIterator2", id(TrieForkJoinListTest::subListListIterator2)),
            argumentSet("spliterator1", id(TrieForkJoinListTest::spliterator1)),
            argumentSet("spliterator2", id(TrieForkJoinListTest::spliterator2)),
            argumentSet("spliterator3", id(TrieForkJoinListTest::spliterator3)),
            argumentSet("spliterator4", id(TrieForkJoinListTest::spliterator4)),
            argumentSet("spliterator5", id(TrieForkJoinListTest::spliterator5)),
            argumentSet("subListSpliterator1", id(TrieForkJoinListTest::subListSpliterator1)),
            argumentSet("safeSizedHandling1", id(TrieForkJoinListTest::safeSizedHandling1))
        );
//        ),
//        IntStream.rangeClosed(1, 100)
//            .mapToObj(i -> argumentSet("fuzz" + i, id(factory -> fuzz(i, factory))))
//        );
    }
    
    // TODO: Add tests for subList methods, reversed list/subList methods, Sizes.OfInt, and a few knickknacks
    
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
        // Concatenate in a pattern such that leftLen < SPAN, but leftLen + rightLen > SPAN
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
    
    static ForkJoinList<Integer> join6(Factory factory) {
        // Join an empty list
        ForkJoinList<Integer> left = listOfSize(factory, 1000);
        ForkJoinList<Integer> right = factory.get();
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join7(Factory factory) {
        // Join onto an empty list
        ForkJoinList<Integer> left = factory.get();
        ForkJoinList<Integer> right = listOfSize(factory, 1000);
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join8(Factory factory) {
        // Join something that is not a ForkJoinList
        ForkJoinList<Integer> left = listOfSize(factory, 1000);
        List<Integer> right = IntStream.range(0, 100).boxed().toList();
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join9(Factory factory) {
        // Join something that does not fork the same kind of ForkJoinList
        ForkJoinList<Integer> left = listOfSize(factory, 1000);
        ForkJoinList<Integer> right = listOfSize(factory, 100).reversed();
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join10(Factory factory) {
        // Join a small (tail-only) list to existing list with a full tail
        ForkJoinList<Integer> left = listOfSize(factory, SPAN*SPAN + SPAN);
        ForkJoinList<Integer> right = listOfSize(factory, SPAN);
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join11(Factory factory) {
        // Join a small (tail-only) list to existing list, new tail fits into existing tail
        ForkJoinList<Integer> left = listOfSize(factory, SPAN*SPAN + SPAN/2);
        ForkJoinList<Integer> right = listOfSize(factory, SPAN/4);
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join12(Factory factory) {
        // Join a small (tail-only) list to existing list, new tail does not fit into existing tail
        ForkJoinList<Integer> left = listOfSize(factory, SPAN*SPAN + SPAN/2);
        ForkJoinList<Integer> right = listOfSize(factory, SPAN);
        left.join(right);
        return left;
    }
    
    static ForkJoinList<Integer> join13(Factory factory) {
        // Join a leftwise-dense tree to a fully-dense tree (result should be leftwise-dense)
        ForkJoinList<Integer> left = listOfSize(factory, SPAN*SPAN);
        ForkJoinList<Integer> right = listOfSize(factory, SPAN*SPAN/2);
        left.join(right);
        return left;
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
    
    static ForkJoinList<Integer> joinAtIndex9(Factory factory) {
        // Join at end (index == size)
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> toInsert = listOfSize(factory, 200);
        list.join(list.size(), toInsert);
        return list;
    }
    
    static ForkJoinList<Integer> joinAtIndex10(Factory factory) {
        // Join an empty list
        ForkJoinList<Integer> left = listOfSize(factory, 1000);
        ForkJoinList<Integer> right = factory.get();
        left.join(300, right);
        return left;
    }
    
    static ForkJoinList<Integer> joinAtIndex11(Factory factory) {
        // Join onto an empty list
        ForkJoinList<Integer> left = factory.get();
        ForkJoinList<Integer> right = listOfSize(factory, 1000);
        left.join(0, right);
        return left;
    }
    
    static ForkJoinList<Integer> joinAtIndex12(Factory factory) {
        // Join something that is not a ForkJoinList
        ForkJoinList<Integer> left = listOfSize(factory, 1000);
        List<Integer> right = IntStream.range(0, 100).boxed().toList();
        left.join(300, right);
        return left;
    }
    
    static ForkJoinList<Integer> joinAtIndex13(Factory factory) {
        // Join something that does not fork the same kind of ForkJoinList
        ForkJoinList<Integer> left = listOfSize(factory, 1000);
        ForkJoinList<Integer> right = listOfSize(factory, 100).reversed();
        left.join(300, right);
        return left;
    }
    
    static ForkJoinList<Integer> add1(Factory factory) {
        // Repeatedly add to a relaxed list, including creating a new root
        int size = SPAN*(SPAN+1);
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(size - 5*SPAN - 3, size - 5*SPAN).clear();
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
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
    
    static Object subListFork1(Factory factory) {
        // Fork interior subList
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> subList = list.subList(100, 9900);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork2(Factory factory) {
        // Fork prefix that ends in the root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> subList = list.subList(0, 9900);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork3(Factory factory) {
        // Fork suffix that starts in the root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> subList = list.subList(100, list.size());
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork4(Factory factory) {
        // Fork empty subList
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> subList = list.subList(100, 100);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork5(Factory factory) {
        // Fork subList over entire list
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        ForkJoinList<Integer> subList = list.subList(0, list.size());
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork6(Factory factory) {
        // Fork prefix that ends in the tail
        int size = SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(0, size - SPAN/2);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork7(Factory factory) {
        // Fork prefix that ends at the tail
        int size = SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(0, size - SPAN);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork8(Factory factory) {
        // Fork prefix of relaxed list
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 1000);
        ForkJoinList<Integer> subList = list.subList(0, 867);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork9(Factory factory) {
        // Fork suffix that starts in the tail
        int size = SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(size - SPAN/2, size);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork10(Factory factory) {
        // Fork subList that starts and ends in the tail
        int size = SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(size - 3*SPAN/4, size - SPAN/4);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork11(Factory factory) {
        // Fork subList that starts in the root and ends at the tail
        int size = SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(size/2, size - SPAN);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork12(Factory factory) {
        // Fork subList that starts in the root and ends in the tail
        int size = SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(size/2, size - SPAN/4);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork13(Factory factory) {
        // Fork subList that starts and ends in the same leaf node in the root
        int size = SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(size/2+2, size/2+4);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork14(Factory factory) {
        // Fork subList of a relaxed list that starts and ends in the root
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 400);
        ForkJoinList<Integer> subList = list.subList(250, 500);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork15(Factory factory) {
        // Fork subList that should leave nothing but a full tail
        int size = SPAN*SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(size - 5*SPAN/2, size - 3*SPAN/2);
        return wrapResultForSubListFork(list, subList);
    }
    
    static Object subListFork16(Factory factory) {
        // Fork subList that should leave nothing but a full root and partial tail
        int size = SPAN*SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        ForkJoinList<Integer> subList = list.subList(size - 5*SPAN/2, size - SPAN);
        return wrapResultForSubListFork(list, subList);
    }
    
    static List<Integer> removeAtIndex1(Factory factory) {
        // Remove all by popping off the end
        ForkJoinList<Integer> list = listOfSize(factory, 100);
        List<Integer> result = new ArrayList<>();
        while (!list.isEmpty()) {
            result.add(list.remove(list.size()-1));
        }
        return result;
    }
    
    static List<Integer> removeAtIndex2(Factory factory) {
        // Remove all by popping off the end (forked list)
        ForkJoinList<Integer> list = listOfSize(factory, 100).fork();
        List<Integer> result = new ArrayList<>();
        while (!list.isEmpty()) {
            result.add(list.remove(list.size()-1));
        }
        return result;
    }
    
    static List<Integer> removeAtIndex3(Factory factory) {
        // Remove all by popping off the end
        // First construct a list with a deep narrow root, to hit special handling as we promote new tails
        int size = SPAN*SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size)
            .subList((size - SPAN)/2, (size + 3*SPAN)/2)
            .fork();
        List<Integer> result = new ArrayList<>();
        while (!list.isEmpty()) {
            result.add(list.remove(list.size()-1));
        }
        return result;
    }
    
    static List<Integer> removeAtIndex4(Factory factory) {
        // Remove all by popping off the front
        ForkJoinList<Integer> list = listOfSize(factory, 100);
        List<Integer> result = new ArrayList<>();
        while (!list.isEmpty()) {
            result.add(list.remove(0));
        }
        return result;
    }
    
    static Object removeAtIndex5(Factory factory) {
        // Remove only element from list that does not own its node (niche code path)
        return listOfSize(factory, 1).fork().removeLast();
    }
    
    static Object removeAtIndex6(Factory factory) {
        // Remove prefix such that there is a thin path to a single leaf node on the left,
        // then remove from the end until we promote that single leaf node to root.
        int size = SPAN*SPAN*SPAN*SPAN;
        List<Integer> list = listOfSize(factory, size);
        list.subList(0, size/2-1).clear();
        while (list.size() > SPAN+1) {
            list.removeLast();
        }
        return list;
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
        // Remove nothing (middle)
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(100, 100).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange6(Factory factory) {
        // Remove prefix ending at tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(0, size - SPAN).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange7(Factory factory) {
        // Remove prefix ending in tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(0, size-1).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange8(Factory factory) {
        // Remove prefix ending in root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(0, 9000).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange9(Factory factory) {
        // Remove suffix starting at tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(size - SPAN, size).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange10(Factory factory) {
        // Remove suffix starting in tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(size-1, size).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange11(Factory factory) {
        // Remove suffix starting in root
        ForkJoinList<Integer> list = listOfSize(factory, 10000);
        list.subList(9000, 10000).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange12(Factory factory) {
        // Remove range that starts and ends in the tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(size-SPAN, size-SPAN/2).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeRange13(Factory factory) {
        // Remove range that starts in the root and ends in the tail
        int size = SPAN*1000;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        list.subList(100, size-SPAN/2).clear();
        return list;
    }
    
    static ForkJoinList<Integer> removeIf1(Factory factory) {
        // Remove evens after prefix
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        list.removeIf(i -> i > 200 && i%2 == 0);
        return list;
    }
    
    static ForkJoinList<Integer> removeIf2(Factory factory) {
        // Remove nothing
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        list.removeIf(i -> i > 2000);
        return list;
    }
    
    static ForkJoinList<Integer> removeAll1(Factory factory) {
        // Remove partially-overlapping collection
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        List<Integer> toRemove = IntStream.range(300, 600).map(i -> i*2).boxed().toList();
        list.removeAll(toRemove);
        return list;
    }
    
    static ForkJoinList<Integer> removeAll2(Factory factory) {
        // Remove non-overlapping collection
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        List<Integer> toRemove = IntStream.range(1200, 1800).boxed().toList();
        list.removeAll(toRemove);
        return list;
    }
    
    static ForkJoinList<Integer> retainAll1(Factory factory) {
        // Retain partially-overlapping collection
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        List<Integer> toRemove = IntStream.range(300, 600).map(i -> i*2).boxed().toList();
        list.retainAll(toRemove);
        return list;
    }
    
    static ForkJoinList<Integer> retainAll2(Factory factory) {
        // Retain non-overlapping collection
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        List<Integer> toRemove = IntStream.range(1200, 1800).boxed().toList();
        list.retainAll(toRemove);
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
        return listOfSize(factory, SPAN*1000).subList(100, 4000).toArray();
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
        zigZagAdd(list, 1000);
        return list;
    }
    
    static ForkJoinList<Integer> listIterator2(Factory factory) {
        // Add all elements via iterator, then remove half via iterator
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 1000);
        int size = list.size();
        ListIterator<Integer> iter = list.listIterator(size/2);
        for (int i = 0; i < size/4; i++) {
            iter.next();
            iter.remove();
            iter.previous();
            iter.remove();
        }
        return list;
    }
    
    static ForkJoinList<Integer> listIterator3(Factory factory) {
        // Add all elements via iterator, then remove all via iterator
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 1000);
        ListIterator<Integer> iter = list.listIterator(list.size()/2);
        for (;;) {
            if (!iter.hasNext()) { break; }
            iter.next();
            iter.remove();
            if (!iter.hasPrevious()) { break; }
            iter.previous();
            iter.remove();
        }
        return list;
    }
    
    static List<Integer> listIterator4(Factory factory) {
        // Use iterator.forEachRemaining() to collect small list into result
        ForkJoinList<Integer> list = listOfSize(factory, SPAN*2);
        ListIterator<Integer> iter = list.listIterator();
        List<Integer> result = new ArrayList<>();
        iter.forEachRemaining(result::add);
        return result;
    }
    
    static List<Integer> listIterator5(Factory factory) {
        // Add all elements via iterator, then forEachRemaining() from middle into result
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 10000);
        ListIterator<Integer> iter = list.listIterator(list.size()/2);
        List<Integer> result = new ArrayList<>();
        iter.forEachRemaining(result::add);
        return result;
    }
    
    static List<Integer> spliterator1(Factory factory) {
        // Consume via a greedy sequential stream that will invoke spliterator.forEachRemaining()
        return listOfSize(factory, 10000).stream()
            .map(i -> i+1)
            .toList();
    }
    
    static List<Integer> spliterator2(Factory factory) {
        // Consume via a short-circuiting sequential stream that will invoke spliterator.tryAdvance()
        return listOfSize(factory, 10000).stream()
            .map(i -> i+1)
            .takeWhile(i -> i < 9999)
            .toList();
    }
    
    static List<Integer> spliterator3(Factory factory) {
        // Consume via a greedy parallel stream that will invoke spliterator.forEachRemaining()
        return listOfSize(factory, 10000).parallelStream()
            .map(i -> i+1)
            .toList();
    }
    
    static List<Integer> spliterator4(Factory factory) {
        // Consume via a short-circuiting parallel stream that will invoke spliterator.tryAdvance()
        return listOfSize(factory, 10000).parallelStream()
            .map(i -> i+1)
            .takeWhile(i -> i < 9999)
            .toList();
    }
    
    static List<Integer> spliterator5(Factory factory) {
        // Test late-binding
        ForkJoinList<Integer> list = factory.get();
        Stream<Integer> stream = list.stream();
        zigZagAdd(list, 10000);
        return stream.map(i -> i+1).parallel().toList();
    }
    
    static List<Integer> subListIterator1(Factory factory) {
        // Remove a bunch of elements via sublist iterator
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        Iterator<Integer> iter = list.subList(400, 600).iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        return list;
    }
    
    static List<Integer> subListIterator2(Factory factory) {
        // Use middle subList iterator.forEachRemaining() to collect elements into result
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 1000);
        Iterator<Integer> iter = list.subList(400, 800).iterator();
        List<Integer> result = new ArrayList<>();
        iter.forEachRemaining(result::add);
        return result;
    }
    
    static List<Integer> subListIterator3(Factory factory) {
        // Use suffix subList iterator.forEachRemaining() to collect elements into result
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 1000);
        Iterator<Integer> iter = list.subList(500, list.size()).iterator();
        List<Integer> result = new ArrayList<>();
        iter.forEachRemaining(result::add);
        return result;
    }
    
    static List<Integer> subListListIterator1(Factory factory) {
        // Add a bunch of elements via sublist iterator
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        ForkJoinList<Integer> subList = list.subList(400, 600);
        zigZagAdd(subList, 1000);
        return list;
    }
    
    static List<Integer> subListListIterator2(Factory factory) {
        // Set a bunch of elements via sublist iterator
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        ListIterator<Integer> iter = list.subList(400, 600).listIterator();
        for (int i = 0; i < 100; i++) {
            iter.set(iter.next() + 1000);
        }
        list.fork(); // Force iterator to invalidate its node ownership
        while (iter.hasNext()) {
            iter.set(iter.next() + 1000);
        }
        return list;
    }
    
    static List<Integer> subListSpliterator1(Factory factory) {
        // Test late-binding
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        ForkJoinList<Integer> subList = list.subList(400, 600);
        Stream<Integer> stream = subList.stream();
        zigZagAdd(subList, 1000);
        return stream.map(i -> i+1).parallel().toList();
    }
    
    static List<Integer> getSet1(Factory factory) {
        // Make a meandering list, reverse it, then add the reversed list to itself
        ForkJoinList<Integer> list = factory.get();
        zigZagAdd(list, 2000);
        Collections.reverse(list); // Calls get() and set()
        for (int i = list.size()-1; i >= 0; i--) {
            list.add(list.get(i));
        }
        return list;
    }
    
    static Object subListSet1(Factory factory) {
        ForkJoinList<Integer> list = listOfSize(factory, 1000);
        ForkJoinList<Integer> subList = list.subList(200, 800);
        subList.fork(); // Share ownership of nodes, forcing set() to path-copy (which is a structural modification)
        return subList.set(300, -1);
    }
    
    static int hashCode1(Factory factory) {
        // Empty list hashCode
        return factory.get().hashCode();
    }
    
    static int hashCode2(Factory factory) {
        // Populated list hashCode
        return listOfSize(factory, 1000).hashCode();
    }
    
    static Object safeSizedHandling1(Factory factory) {
        // This is a fairly brittle test, in the sense that several seemingly innocuous changes to
        // the implementation can mask the issue we are trying to express without actually fixing it.
        // For now, the steps to reproduce look like:
        //   1. Create a list with a gap, inducing sized nodes
        //   2. Fill the gap in such a way that the implementation does not replace the (now-full) sized nodes with not-sized nodes
        //   3. Induce a concatenation that will replace the parent of a (full) sized node with a not-sized node(!)
        //   4. Attempt to access an element under the (full) sized node, causing IOOBE due to index mismanagement
        // Possible non-fixes include:
        //   A. In (2), implementation replaces full sized nodes with not-sized nodes in test case, but may not in other cases
        //   B. In (3), implementation does not replace the parent with a not-sized node in test case, but may in other cases
        // Possible fixes include:
        //   A. In (2), ensure the implementation always replaces full sized nodes with not-sized nodes
        //   B. In (4), when traversing to an index, ensure index is adjusted to handle sized nodes below not-sized nodes
        
        int size = SPAN*SPAN*SPAN;
        ForkJoinList<Integer> list = listOfSize(factory, size);
        List<Integer> toInsert = IntStream.range(0, SPAN).boxed().toList();
        list.remove(size/4); // Add gap to induce sized parent/grandparent nodes
        list.listIterator(size/4).add(7); // Remove gap, possibly without converting nodes back to not-sized
        list.addAll(size - 2*SPAN, toInsert); // Induce concatenation that makes grandparent not-sized
        return list.get(size/4); // Attempt to access element through not-sized -> sized path, possibly triggering IOOBE
    }
    
    @Test
    void testEquals1() {
        // List equals itself
        List<Integer> list = listOfSize(TRIE_FJL, 100);
        assertTrue(list.equals(list));
    }
    
    @Test
    void testEquals2() {
        // List does not equal non-List
        List<Integer> list = listOfSize(TRIE_FJL, 100);
        Set<Integer> set = new HashSet<>(list);
        assertFalse(list.equals(set));
        assertFalse(set.equals(list));
    }
    
    @Test
    void testEquals3() {
        // List does not equal other List with different size
        List<Integer> list1 = listOfSize(TRIE_FJL, 100);
        List<Integer> list2 = listOfSize(TRIE_FJL, 80);
        assertFalse(list1.equals(list2));
        assertFalse(list2.equals(list1));
    }
    
    @Test
    void testEquals4() {
        // List does not equal other List with different elements
        List<Integer> list1 = IntStream.range(0, 100).boxed().collect(Collectors.toCollection(TrieForkJoinList::new));
        List<Integer> list2 = IntStream.range(0, 100).boxed().collect(Collectors.toCollection(ArrayList::new));
        list2.set(60, 1);
        assertFalse(list1.equals(list2));
        assertFalse(list2.equals(list1));
    }
    
    @Test
    void testEquals5() {
        // List equals other List with same elements
        List<Integer> list1 = IntStream.range(0, 100).boxed().collect(Collectors.toCollection(TrieForkJoinList::new));
        List<Integer> list2 = IntStream.range(0, 100).boxed().collect(Collectors.toCollection(ArrayList::new));
        assertTrue(list1.equals(list2));
        assertTrue(list2.equals(list1));
    }
    
    @Test
    void testGetFirstLast1() {
        List<Integer> list = listOfSize(TRIE_FJL, 100);
        assertEquals(0, list.getFirst());
        assertEquals(99, list.getLast());
        list.set(0, 6);
        list.set(list.size()-1, 8);
        assertEquals(6, list.getFirst());
        assertEquals(8, list.getLast());
    }
    
    @Test
    void testGetFirstLast2() {
        // Small (tail-only) list
        List<Integer> list = listOfSize(TRIE_FJL, 3);
        assertEquals(0, list.getFirst());
        assertEquals(2, list.getLast());
        list.set(0, 4);
        list.set(list.size()-1, 9);
        assertEquals(4, list.getFirst());
        assertEquals(9, list.getLast());
    }
    
    @Test
    void testGetFirstLast3() {
        List<Integer> list = new TrieForkJoinList<>();
        assertThrows(NoSuchElementException.class, list::getFirst);
        assertThrows(NoSuchElementException.class, list::getLast);
    }
    
    @Test
    void testSizeTooBig() {
        ForkJoinList<Integer> list = new TrieForkJoinList<>(List.of(1));
        assertThrows(OutOfMemoryError.class, () -> {
            // Last join will attempt to set size = 2^31 > Integer.MAX_VALUE = 2^31-1
            // The actual memory usage here is pretty reasonable due to structural sharing,
            // but the implementation is not designed to support sizes > Integer.MAX_VALUE,
            // and will throw OOME to protect itself.
            // (java.util.ArrayList also does this, but would necessarily consume a lot of memory to get to that size.)
            for (int i = 1; i <= 31; i++) {
                list.join(list);
            }
        });
    }
    
    @Test
    void testJoinOutOfBounds() {
        ForkJoinList<Integer> list = new TrieForkJoinList<>();
        ForkJoinList<Integer> toInsert = listOfSize(TRIE_FJL, 10);
        assertThrows(IndexOutOfBoundsException.class, () -> list.join(1, toInsert));
    }
    
    @Test
    void testSubListOutOfBounds1() {
        // To index out of bounds
        ForkJoinList<Integer> list = listOfSize(TRIE_FJL, 10);
        assertThrows(IndexOutOfBoundsException.class, () -> list.subList(5, 15));
    }
    
    @Test
    void testSubListOutOfBounds2() {
        // From index out of bounds
        ForkJoinList<Integer> list = listOfSize(TRIE_FJL, 10);
        assertThrows(IndexOutOfBoundsException.class, () -> list.subList(-5, 10));
    }
    
    @Test
    void testSubListOutOfBounds3() {
        // From index > to index
        ForkJoinList<Integer> list = listOfSize(TRIE_FJL, 10);
        assertThrows(IllegalArgumentException.class, () -> list.subList(8, 5));
    }
    
    @Test
    void testSpliteratorTooSmallToSplit() {
        assertNull(new TrieForkJoinList<>(List.of(1)).spliterator().trySplit());
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
    
    static void zigZagAdd(List<Integer> list, int initialLinearSpan) {
        ListIterator<Integer> iter = list.listIterator();
        int i;
        for (i = 0; i < initialLinearSpan; i++) {
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
    }
    
    // Sublist fork tests return both original list and subList fork,
    // after updating all elements in both to verify no interference across the fork
    static Object wrapResultForSubListFork(ForkJoinList<Integer> list, ForkJoinList<Integer> subList) {
        subList = subList.fork(); // Updates to original list should not affect this fork, and vice versa
        list.replaceAll(i -> i+1);
        subList.replaceAll(i -> i-1);
        return List.of(list, subList);
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
        public boolean join(Collection<? extends E> c) {
            return base.addAll(c);
        }
        
        @Override
        public boolean join(int index, Collection<? extends E> c) {
            return base.addAll(index, c);
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