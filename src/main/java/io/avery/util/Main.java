package io.avery.util;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

public class Main {
    // TODO: Difference between add(i,e) with directAppend vs concat, esp. when leaf is full but ancestors are not?
    //  Depends which condition is hit first:
    //  1. If a right-edge node is not full, then we empty new single-child right sibling into it.
    //     - This matches the behavior of directAppend
    //  2. If a right-edge node is full, but its left siblings are not, such that the new single-child right sibling
    //     triggers a rebalance (at the parent level) - then we shift children left, eliminating at least one sibling,
    //     which ensures room for the new right sibling in the parent.
    //     - This effect is not replicated by directAppend
    //     - If we are (bulk) appending the right number of elements, or sufficiently many elements (such that new right
    //       siblings are full), then the rebalance condition would not have triggered anyway.
    //     - Else we may miss out on rebalancing left, and subsequent concatenation may not rectify the scarcity of the
    //       the new right sibling(s) children (that would have triggered the missed rebalance).
    //     - *Iterator.add seems to behave similarly, minus the right-side concatenation 'afterward'.
    //
    // directAppend is justified by being at least as good as forkPrefix + appends (push-down tail)
    
    // TODO: Where we are calling concatSubTree, can this result in a non-full rightmost leaf?
    //  - Seemingly not in join(), assuming both sides start with a full rightmost leaf
    
    public static void main(String[] args) {
        iterFuzz();
    }
    
    static void iterFuzz() {
        List<Integer> bootstrap = IntStream.range(0, 100_000_000).boxed().toList();
//        Instant start = Instant.now();
//        List<Integer> list = new ArrayList<>(bootstrap);
        List<Integer> list = new TrieForkJoinList<>(bootstrap);
//        List<Integer> list = new LinkedList<>(bootstrap);
//        Instant start = Instant.now();
//        for (int i = 0; i < 100_000_000; i++) {
//            list.add(0, i);
//        }
        Instant start = Instant.now();
        var state = new Object(){ long sum = 0; };
        list.forEach(i -> state.sum += i);
//        list.iterator().forEachRemaining(i -> state.sum += i);
//        for (int i : list) {
//            state.sum += i;
//        }
//        for (var iter = list.listIterator(); iter.hasNext(); ) {
//            int i = iter.next();
//            state.sum += i;
//            iter.set(i+1);
//        }
        Instant end = Instant.now();
        System.out.println(state.sum);
        System.out.println(Duration.between(start, end));
    }
    
    static void iterFuzz2() {
        // TFJL       = PT2M6.968707S
        // LinkedList = PT1.437222S
        // ArrayList  = (killed after 10 min)
        List<Integer> bootstrap = IntStream.range(0, 50_000_000).boxed().toList();
        List<Integer> list = new TrieForkJoinList<>(bootstrap);
        var iter = list.listIterator(list.size());
        Instant start = Instant.now();
        for (;;) {
            if (!iter.hasPrevious()) break;
            iter.previous();
            iter.add(0);
            if (!iter.hasPrevious()) break;
            iter.previous();
        }
        Instant end = Instant.now();
        System.out.println(Duration.between(start, end));
    }
    
    static void removeLast() {
        ForkJoinList<Integer> list = new TrieForkJoinList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        list.fork();
        for (int i = 0; i < 1000; i++) {
            System.out.println(list.removeLast());
        }
    }
    
    static void shuffle() {
        ForkJoinList<Integer> list = new TrieForkJoinList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        for (int i = 0; i < 999; i += 3) {
            int a = list.get(i), b = list.get(i+1), c = list.get(i+2);
            list.set(i, c);
            list.set(i+1, b);
            list.set(i+2, a);
        }
        for (int i = 0; i < 1000; i++) {
            System.out.println(list.get(i));
        }
    }
    
    static void forkShuffle() {
        ForkJoinList<Integer> list = new TrieForkJoinList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        ForkJoinList<Integer> copy = list.fork();
        for (int i = 0; i < 999; i += 3) {
            int a = list.get(i), b = list.get(i+1), c = list.get(i+2);
            list.set(i, c);
            list.set(i+1, b);
            list.set(i+2, a);
        }
        for (int i = 0; i < 1000; i++) {
            System.out.println(copy.get(i) + " " + list.get(i));
        }
    }
    
    static void printSum() {
        Random rand = new Random();
        ForkJoinList<Integer> list = new TrieForkJoinList<>();
        for (int i = 0; i < 100000000; i++) {
            list.add(rand.nextInt(0, Integer.MAX_VALUE));
        }
        long sum = 0;
        for (int i = 0; i < 100000000; i++) {
            sum += list.get(i);
            if (i % 1000000 == 0) System.out.println(i);
        }
        System.out.println(sum);
    }
}