package org.example;

public class Resource2 {
    public static synchronized String getResource() throws InterruptedException {
        Thread.sleep(2000);
        Resource1.getResource();
        return Thread.currentThread().getName();
    }
}
