package com.spr.ops.executor;

import com.spr.core.logger.Logger;
import com.spr.core.logger.LoggerFactory;
import com.spr.exception.utils.ExceptionUtils;
import com.spr.ops.ContextTransferableCallable;
import com.spr.ops.executor.util.SprinklrThreadFactory;
import com.spr.utils.EnvironmentUtils;
import com.spr.utils.ThreadUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractSprinklrExecutorService implements SprinklrExecutorService {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractSprinklrExecutorService.class);

    public abstract ExecutorService getExecutorService();

    public abstract String getType();

    /**
     * Executes a series of tasks in parallel and return results.
     *
     * @param tasks tasks list
     */
    @Override
    public <T> Collection<T> execute(Collection<? extends Callable<T>> tasks) {
        warnIfRecursiveCall();
        if (CollectionUtils.isEmpty(tasks)) {
            return Collections.emptyList();
        }

        Collection<T> result = new ArrayList<>();
        try {
            Collection<Future<T>> futures = getExecutorService().invokeAll(tasks);
            for (Future<T> future : futures) {
                T t = future.get();
                if (t != null) {
                    result.add(t);
                }
            }
            return result;
        } catch (Exception e) {
            logger.error("Error occured while executing parallel tasks.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * @param task task to execute
     */
    @Override
    public <T> T execute(Callable<T> task) {
        warnIfRecursiveCall();
        Future<T> future = getExecutorService().submit(task);
        try {
            return future.get();
        } catch (Exception e) {
            logger.error("Error occured while executing parallel tasks.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T execute(Callable<T> task, long maxTimeOut, TimeUnit timeUnit) {
        return execute(task, maxTimeOut, timeUnit, false);
    }

    @Override
    public <T> T execute(Callable<T> task, long maxTimeOut, TimeUnit timeUnit, boolean cancel) {
        warnIfRecursiveCall();
        Future<T> future = getExecutorService().submit(task);
        try {
            return future.get(maxTimeOut, timeUnit);
        } catch (NullPointerException eX) {
            logger.error("Error occured while executing parallel tasks.", ExceptionUtils.getStackTrace(eX));
            if (cancel) {
                future.cancel(true);
            }
            throw new RuntimeException(eX);
        } catch (Exception e) {
            logger.error("Error occured while executing parallel tasks.", e);
            if (cancel) {
                future.cancel(true);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Blocking call
     */
    @Override
    public <T> Collection<Future<T>> submit(Collection<? extends Callable<T>> tasks) {
        warnIfRecursiveCall();
        try {
            return getExecutorService().invokeAll(tasks);
        } catch (InterruptedException e) {
            logger.error("Error occured while executing parallel tasks.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        warnIfRecursiveCall();
        return getExecutorService().submit(task);
    }

    @Override
    public void submit(Runnable runnable) {
        warnIfRecursiveCall();
        getExecutorService().submit(runnable);
    }

    @Override
    public Future<?> submitRunnable(Runnable runnable) {
        warnIfRecursiveCall();
        return getExecutorService().submit(runnable);
    }

    public void execute(Runnable runnable) {
        warnIfRecursiveCall();
        getExecutorService().execute(runnable);
    }

    @Override
    public void shutdown() {
        //The thread pool is cached by a singleton executor service. so this operation may not be required
        throw new UnsupportedOperationException();
    }

    @Override
    public int getActiveCount() {
        return getActiveCount(getExecutorService());
    }

    public static int getActiveCount(ExecutorService executorService) {
        if (executorService instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
            return threadPoolExecutor.getActiveCount();
        }
        return -1; // otherwise unknown
    }

    public static int getQueueSize(ExecutorService executorService) {
        if (executorService instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
            return threadPoolExecutor.getQueue().size();
        }
        return -1; // otherwise unknown
    }

    @Override
    public int getPoolSize() {
        final ExecutorService executorService = getExecutorService();
        if (executorService instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
            return threadPoolExecutor.getPoolSize();
        }
        return -1; // otherwise unknown
    }

    protected void warnIfRecursiveCall() {
        if (EnvironmentUtils.isQAEnv()) {
            try {
                final String currentThreadName = Thread.currentThread().getName();
                if (StringUtils.isNotBlank(currentThreadName) && StringUtils.isNotBlank(getType())
                        && currentThreadName.startsWith(getType() + "-pool-") && logger.isDebugEnabled()) {
                    logger.debug("ALERT! Waiting for deadlock to happen", new Exception());
                }
            } catch (Exception eX) {
                // do nothing.
            }
        }
    }

    protected ThreadFactory getThreadFactory() {
        return StringUtils.isBlank(getType()) ? new SprinklrThreadFactory() : new SprinklrThreadFactory(getType());
    }

    protected RejectedExecutionHandler getBlockingPutRejectedExecutionHandler() {
        return ThreadUtil.createBlockingPutRejectedExecutionHandler(StringUtils.isBlank(getType()) ? "SPR-APP" : getType());
    }

    public <Item> void safeExecute(List<Item> itemList, Consumer<Item> consumer) {
        safeExecute(itemList, consumer, t -> true);
    }

    public <Item> void safeExecute(List<Item> itemList, Consumer<Item> consumer, Predicate<Item> waitOnCondition) {

        List<Future<Void>> futures = new ArrayList<>();
        for (Item item : itemList) {
            Future<Void> submit = submit(new ContextTransferableCallable<Void>() {
                @Override
                public Void doCall() {
                    consumer.accept(item);
                    return null;
                }
            });
            if (waitOnCondition.test(item)) {
                futures.add(submit);
            }
        }
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                LOGGER.error("Error while executing ", e);
            }
        }
    }

    public static <T> void waitExecutionSafe(List<Future<T>> futures) {
        for (Future<T> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static void waitExecution(Collection<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw ExceptionUtils.wrapInRuntimeExceptionIfNecessary(e);
            }
        }
    }

    public static void waitTasksExecution(Collection<FutureTask<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw ExceptionUtils.wrapInRuntimeExceptionIfNecessary(e);
            }
        }
    }

    public static void waitExecutionSafeRunnable(List<Future> futures) {
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static void waitExecutionAndCancelIfRemaining(List<Future> futures, long maxWaitTimeInMillis) {
        Long maxTimestampToWait = maxWaitTimeInMillis + System.currentTimeMillis();
        int remainingFutures = futures.size();
        for (Future future : futures) {
            try {
                final long maxWaitTime = maxTimestampToWait - System.currentTimeMillis();
                if (maxWaitTime < 0) {
                    logger.error("Timed out while waiting for futures: " + remainingFutures + " cancelling remaining");
                    future.cancel(true);
                    continue;
                }
                future.get(maxWaitTime, TimeUnit.MILLISECONDS);
                remainingFutures--;
            } catch (Exception e) {
                logger.error("Exception while waiting for futures: " + remainingFutures, e);
            }
        }
    }

    public static <T> void waitExecutionSafe(List<Future<T>> futures, long maxWaitTimeInMillis) {
        Long maxTimestampToWait = maxWaitTimeInMillis + System.currentTimeMillis();
        for (Future future : futures) {
            try {
                final long maxWaitTime = maxTimestampToWait - System.currentTimeMillis();
                if (maxWaitTime < 0) {
                    break;
                }
                future.get(maxWaitTime, TimeUnit.MILLISECONDS);

            } catch (Exception e) {
                logger.debug(e.getMessage(), e);
            }
        }
    }

    /**
     * A handler for rejected tasks that silently discards the
     * rejected task.
     */
    public static class SprDiscardPolicy implements RejectedExecutionHandler {

        /**
         * Creates a {@code DiscardPolicy}.
         */
        public SprDiscardPolicy() {
        }

        /**
         * Does nothing, which has the effect of discarding task r.
         *
         * @param r the runnable task requested to be executed
         * @param e the executor attempting to execute this task
         */
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            logger.error("Discarding batch rejected execution.", new Exception());
        }
    }
}