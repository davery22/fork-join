package io.avery.util;

import java.util.*;

public class Main {
    public static void main(String[] args) {
        join2();
//        join();
//        shuffle();
//        forkShuffle();
//        removeLast();
    }
    
    static void join() {
        ForkJoinList<Integer> left = new TrieForkJoinList<>();
        ForkJoinList<Integer> right = new TrieForkJoinList<>();
        for (int i = 0; i < 1000; i++) {
            left.add(i);
            right.add(2000-i);
        }
        left.join(right);
        for (int i = 0; i < left.size(); i++) {
            System.out.println(left.get(i));
        }
    }
    
    static void join2() {
        ForkJoinList<Integer> left = new TrieForkJoinList<>();
        ForkJoinList<Integer> right = new TrieForkJoinList<>();
        for (int i = 0; i < 1000; i++) {
            left.add(i);
            right.add(2000-i);
        }
        left.join(right);
        left.join(left);
        for (int i = 0; i < left.size(); i++) {
            System.out.println(left.get(i));
        }
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
    
    static class ArrayForkJoinList<E> implements ForkJoinList<E> {
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