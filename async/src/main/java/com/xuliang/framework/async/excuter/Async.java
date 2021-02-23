package com.xuliang.framework.async.excuter;


import com.xuliang.framework.async.wrapper.WorkerWrapper;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 类入口，可以根据自己情况调整core线程的数量
 *
 * @author xuliang
 * @version 1.0
 */
public class Async {

    public static final ThreadPoolExecutor COMMON_POOL =
            new ThreadPoolExecutor(500, 2000,
                    15L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(3000),
                    (ThreadFactory) Thread::new);


    /**
     * 同步阻塞,直到所有都完成,或失败
     */
    public static boolean beginWork(long timeout, WorkerWrapper... workerWrapper) throws ExecutionException, InterruptedException {
        return beginWork(timeout, COMMON_POOL, workerWrapper);
    }

    /**
     * 如果想自定义线程池，请传pool。不自定义的话，就走默认的COMMON_POOL
     */
    public static boolean beginWork(long timeout, ThreadPoolExecutor pool, WorkerWrapper... workerWrapper) throws ExecutionException, InterruptedException {
        if (workerWrapper == null || workerWrapper.length == 0) {
            return false;
        }
        List<WorkerWrapper> workerWrappers = Arrays.stream(workerWrapper).collect(Collectors.toList());
        return beginWork(timeout, pool, workerWrappers);
    }


    /**
     * 开始工作
     */
    public static boolean beginWork(long timeout, ThreadPoolExecutor pool, List<WorkerWrapper> workerWrappers) throws ExecutionException, InterruptedException {
        if (workerWrappers == null || workerWrappers.size() == 0) {
            return false;
        }
        //定义一个map，存放所有的wrapper，key为wrapper的唯一id，value是该wrapper，可以从value中获取wrapper的result
        Map<String, WorkerWrapper> forParamUseWrappers = new ConcurrentHashMap<>();
        CompletableFuture[] futures = new CompletableFuture[workerWrappers.size()];
        for (int i = 0; i < workerWrappers.size(); i++) {
            WorkerWrapper wrapper = workerWrappers.get(i);
            futures[i] = CompletableFuture.runAsync(() -> wrapper.work(pool, timeout, forParamUseWrappers), pool);
        }
        try {
            CompletableFuture.allOf(futures).get(timeout, TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            Set<WorkerWrapper> set = new HashSet<>();
            totalWorkers(workerWrappers, set);
            System.err.println("=== 总控制台超时，停止所有任务初始化/正在执行中的任务，当前任务数："+set.size()+" ===");
            for (WorkerWrapper wrapper : set) {
                wrapper.stopNow();
            }
            return false;
        }
    }


    /**
     * 总共多少个执行单元
     */
    @SuppressWarnings("unchecked")
    private static void totalWorkers(List<WorkerWrapper> workerWrappers, Set<WorkerWrapper> set) {
        set.addAll(workerWrappers);
        for (WorkerWrapper wrapper : workerWrappers) {
            if (wrapper.getNextWrappers() == null) {
                continue;
            }
            List<WorkerWrapper> wrappers = wrapper.getNextWrappers();
            totalWorkers(wrappers, set);
        }

    }


    public static void shutDown() {
        COMMON_POOL.shutdown();
    }


    /**
     * 获取当前线程数量
     */
    public static String getThreadCount() {
        return "activeCount=" + COMMON_POOL.getActiveCount() +
                "  completedCount " + COMMON_POOL.getCompletedTaskCount() +
                "  largestCount " + COMMON_POOL.getLargestPoolSize();
    }


}
