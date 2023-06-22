package com.spr.ops.executor;

import com.spr.core.logger.Logger;
import com.spr.core.logger.LoggerFactory;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Sprinklr Executor service that internally uses Executors to execute tasks in parallel and club the results.
 *
 * @author rachit
 */
public interface SprinklrExecutorService {

    Logger LOGGER = LoggerFactory.getLogger();

    /**
     * Returns the future for a task, helpful at times when someone wants to cancel the task.
     */
    <T> Future<T> submit(Callable<T> task);

    /**
     * Executes some tasks and return the future.
     */
    public <T> Collection<Future<T>> submit(Collection<? extends Callable<T>> tasks);

    /**
     * Executes a task and return the result, will wait for task completion.
     *
     * @param task task to execute
     */
    public <T> T execute(Callable<T> task);

    public <T> T execute(Callable<T> task, long maxTimeOut, TimeUnit timeUnit);

    <T> T execute(Callable<T> task, long maxTimeOut, TimeUnit timeUnit, boolean cancel);

    /**
     * Executes a series of tasks in parallel, wait for all to complete and return results.
     *
     * @param tasks tasks list to execute
     */
    public <T> Collection<T> execute(Collection<? extends Callable<T>> tasks);

    /**
     * Submit a runnable and let is execute.
     * @param runnable The {@link Runnable} instance to execute.
     */
    void submit(Runnable runnable);

    /**
     * Submit a runnable and returns the future for a runnable
     *
     * @param runnable The {@link Runnable} instance to execute.
     */
    Future<?> submitRunnable(Runnable runnable);

    /**
     * Shut down the executor service
     */
    void shutdown();

    /**
     * Use primarily for logging purposes and getting a view of the current executor.
     */
    int getActiveCount();

    int getPoolSize();

    static void waitExecution(Collection<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

}