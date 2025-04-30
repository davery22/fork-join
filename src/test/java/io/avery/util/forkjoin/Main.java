/*
 * MIT License
 *
 * Copyright (c) 2025 Daniel Avery
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.avery.util.forkjoin;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        streamsBench();
    }
    
    static void streamsBench() {
        var collector = Collector.of(
            TrieForkJoinList::new,
            List::add,
            (a, b) -> { a.join(b); return a; },
            Function.identity()
        );
        int size = 10_000_000;
        int sum = 0;
        for (int i = 0; i < 500; i++) {
            var list = IntStream.range(0, size).boxed().parallel()
                .filter(j -> j % 1001 != 0)
//                .collect(collector);
                .toList();
            sum += list.size();
        }
        System.out.println(sum);
    }
    
    static void zigZagAddBench() {
        int size = 10;
        int sum = 0;
        for (int i = 0; i < 10_000_000; i++) {
//            List<Integer> list = new LinkedList<>();
            List<Integer> list = new ArrayList<>(); // ~24 sec @size=1k,i=100k
//            List<Integer> list = new TrieForkJoinList<>(); // ~96 sec
            zigZagAdd(list, size);
            sum += list.size();
        }
        System.out.println(sum);
    }
    
    static void joinBench2() {
        int size = 1_000;
        List<Integer> bootstrap = IntStream.range(0, size).boxed().toList();
        ForkJoinList<Integer> right = new TrieForkJoinList<>(bootstrap);
//        List<Integer> right = new ArrayList<>(bootstrap);
        int sum = 0;
        for (int j = 0; j < 100; j++) {
            ForkJoinList<Integer> left = new TrieForkJoinList<>(); // ~1.3 sec spent in addAll, ~.7 sec spent in join
//            List<Integer> left = new ArrayList<>(); // ~8 sec spent in addAll
            for (int i = 0; i < 100_000; i++) {
                left.join(right);
//                left.addAll(right);
                sum += left.size();
            }
        }
        System.out.println(sum);
    }
    
    static void joinBench() {
        int size = 10_000;
        List<Integer> bootstrap1 = IntStream.range(0, size).boxed().toList();
        List<Integer> bootstrap2 = IntStream.range(0, size).boxed().toList();
//        List<Integer> list1 = new ArrayList<>(bootstrap1);
//        List<Integer> list2 = new ArrayList<>(bootstrap2);
        ForkJoinList<Integer> list1 = new TrieForkJoinList<>(bootstrap1);
        ForkJoinList<Integer> list2 = new TrieForkJoinList<>(bootstrap2);
        long start = System.nanoTime();
//        Instant start = Instant.now();
//        list1.addAll(list2);
        list1.join(list2);
        long end = System.nanoTime();
//        Instant end = Instant.now();
        System.out.println(list1.size());
        System.out.println((double) (end - start)/1e9 + " S");
//        System.out.println(Duration.between(start, end));
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
}