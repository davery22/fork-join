package io.avery.util;

import java.util.Random;

public class Main {
    public static void main(String[] args) {
        removeLast();
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