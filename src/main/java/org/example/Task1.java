package org.example;

import java.util.concurrent.Callable;

public class Task1 implements Callable<String> {
    @Override
    public String call() throws Exception {
        return "Hello from " + Thread.currentThread().getName() + " for Task1"
                + "Resource = " + Resource1.getResource();
    }
}
