package org.example;

import java.util.concurrent.Callable;

public class Task2 implements Callable<String> {
    @Override
    public String call() throws Exception {
        return "Hello from " + Thread.currentThread().getName() + " for Task2"
                + "Resource = " + Resource1.getResource();
    }
}
