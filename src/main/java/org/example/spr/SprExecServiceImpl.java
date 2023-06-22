package com.spr.ops.executor;

import com.spr.props.SprProperties;
import com.spr.props.SprinklrProperties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Rachit
 * Date: 21/05/12
 * Time: 11:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class SprinklrExecutorServiceImpl extends AbstractSprinklrExecutorService {

    private static final SprinklrExecutorServiceImpl INSTANCE = new SprinklrExecutorServiceImpl();

    public static SprinklrExecutorServiceImpl getInstance() {
        return INSTANCE;
    }

    private volatile ExecutorService service;

    private SprinklrExecutorServiceImpl() {
    }

    @Override
    public ExecutorService getExecutorService() {
        if (service == null) {
            synchronized (this) {
                if (service == null) {
                    final SprinklrProperties properties = SprProperties.getSprProperties();
                    int corePoolSize = properties.getInt("sprinklr.threadpool.corePoolSize", 200);
                    int maxPoolSize = getMaxPoolSize();
                    long keepAliveTime = properties.getLong("sprinklr.threadpool.keepAliveTime", 2L);
                    final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(getWorkQueueSize());

                    if (maxPoolSize < corePoolSize) {
                        maxPoolSize = corePoolSize;
                    }

                    ThreadPoolExecutor poolExecutor =
                        new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MINUTES, workQueue, getThreadFactory(),
                                               getBlockingPutRejectedExecutionHandler());
                    if (keepAliveTime > 0) {
                        poolExecutor.allowCoreThreadTimeOut(properties.getBoolean("sprinklr.threadpool.allowCoreThreadTimeOut", true));
                    }
                    service = poolExecutor;
                }
            }
        }
        return service;
    }

    @Override
    public String getType() {
        return "SPR-APP";
    }

    public static Integer getMaxPoolSize() {
        return SprProperties.getSprProperties().getInt("sprinklr.threadpool.maxPoolSize", 1000);
    }

    public static Integer getWorkQueueSize() {
        return SprProperties.getSprProperties().getInt("sprinklr.threadpool.workQueueSize", 5);
    }

}