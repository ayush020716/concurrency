package org.example;

import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ThreadPoolExecutor threadPoolExecutor1 = new ThreadPoolExecutor(
                0,
                1,
                10,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(1)
        );
        ThreadPoolExecutor threadPoolExecutor2 = new ThreadPoolExecutor(
                0,
                1,
                10,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(1)
        );
        Future<String> future1 = threadPoolExecutor1.submit(new Task1());
        Future<String> future2 = threadPoolExecutor2.submit(new Task2());
        System.out.println(future1.get());
        System.out.println(future2.get());
        threadPoolExecutor1.shutdown();
        threadPoolExecutor2.shutdown();
    }
}