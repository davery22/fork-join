package io.avery.util.forkjoin;

import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 5)
@Fork(value = 3)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Bench1 {
//    @Param({"100", "1000", "10000", "100000"})
//    @Param({"100", "1000", "10000", "100000", "1000000"})
//    @Param({"100", "1000", "10000", "100000", "1000000", "10000000"})
    @Param({"100", "1000", "10000", "100000", "1000000", "10000000", "100000000"})
    public int n;
    
    List<Integer> toInsert;
    List<Integer> fjlToInsert;
    
    @Setup(Level.Trial)
    public void setupBootstrapList() {
        fjlToInsert = new TrieForkJoinList<>(toInsert = IntStream.range(0, n).boxed().toList());
    }
    
//    List<Integer> list;

//    @Setup(Level.Iteration)
//    public void setupReusedList() {
//        list = new TrieForkJoinList<>(toInsert);
//    }
    
//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchAppends() {
//        var list = new TrieForkJoinList<>();
//        for (int i = 0; i < n; i++) {
//            list.add(i);
//        }
//        return list;
//    }
    
//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchPrepends() {
//        var list = new TrieForkJoinList<Integer>();
//        for (int i = 0; i < n; i++) {
//            list.add(0, i);
//        }
//        return list;
//    }

//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchBulkAppend() {
//        var list = new TrieForkJoinList<>(toInsert);
//        list.addAll(toInsert);
//        //list.join(fjlToInsert);
//        return list;
//    }
    
//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchBulkPrepend() {
//        var list = new TrieForkJoinList<>(toInsert);
//        list.addAll(0, toInsert);
//        //list.join(0, fjlToInsert);
//        return list;
//    }

//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchInsert() {
//        var list = new TrieForkJoinList<>(toInsert);
//        for (int size = n, i = 0; i < n; i++) {
//            list.add(size++/2, i);
//        }
//        return list;
//    }

//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchDelete() {
//        var list = new TrieForkJoinList<>(toInsert);
//        for (int size = n, i = 0; i < n/2; i++) {
//            list.remove(size--/2);
//        }
//        return list;
//    }

//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchBulkInsert() {
//        var list = new TrieForkJoinList<>(toInsert);
//        list.addAll(n/2, toInsert);
//        //list.join(n/2, fjlToInsert);
//        return list;
//    }

//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchBulkDelete() {
//        var list = new TrieForkJoinList<>(toInsert);
//        list.subList(n/4, 3*n/4).clear();
//        return list;
//    }
    
//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchFor() {
//        int sum = 0;
//        for (var i : list) {
//            sum += i;
//        }
//        return sum;
//    }
    
//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchForEach() {
//        var s = new Object(){ int sum = 0; };
//        list.forEach(i -> s.sum += i);
//        return s.sum;
//    }
    
//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchReplaceAll() {
//        list.replaceAll(i -> i+1);
//        return list;
//    }

//    @Benchmark
//    @BenchmarkMode(value = Mode.SampleTime)
//    public Object benchBulkLoad() {
//        return new TrieForkJoinList<>(toInsert);
//    }
    
    @Benchmark
    @BenchmarkMode(value = Mode.SampleTime)
    public Object benchCollect() {
        //return toInsert.stream().toList();
        //return toInsert.stream().parallel().toList();
        //return toInsert.stream().parallel().filter(i -> true).toList();
        //return toInsert.stream().collect(Collectors.toList());
        //return toInsert.stream().parallel().collect(Collectors.toList());
        //return toInsert.stream().collect(Collectors.toCollection(TrieForkJoinList::new));
        //return toInsert.stream().parallel().collect(Collectors.toCollection(TrieForkJoinList::new));
        //return toInsert.stream().parallel().collect(joiningCollector());
        return toInsert.stream().parallel().filter(i -> true).collect(joiningCollector());
    }
    
    private <T> Collector<T,?,ForkJoinList<T>> joiningCollector() {
        return Collector.of(
            TrieForkJoinList::new, List::add,
            (a, b) -> { a.join(b); return a; },
            Function.identity()
        );
    }
}
