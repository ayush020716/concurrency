package org.example;

public class Resource1 {
    public static synchronized String getResource() throws InterruptedException {
        Thread.sleep(2000);
        Resource2.getResource();
        return Thread.currentThread().getName();
    }
}
