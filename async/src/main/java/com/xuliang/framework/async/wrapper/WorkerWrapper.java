package com.xuliang.framework.async.wrapper;


import com.xuliang.framework.async.callback.DefaultCallback;
import com.xuliang.framework.async.callback.ICallBack;
import com.xuliang.framework.async.callback.IWorker;
import com.xuliang.framework.async.depend.DependWrapper;
import com.xuliang.framework.async.excuter.timer.SystemClock;
import com.xuliang.framework.async.worker.ResultState;
import com.xuliang.framework.async.worker.WorkResult;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 对worker进行封装 处理
 *
 * @author xuliang
 */
public class WorkerWrapper<T, V> {

    /**
     * 该wrapper 的唯一标识
     */
    private String id;

    /**
     * worker处理的参数
     */

    private T param;

    /**
     * 任务名称
     */
    private String name;

    /**
     * 具体处理的worker
     */
    private IWorker<T, V> iWorker;

    /**
     * 处理回调的callback
     */
    private ICallBack<T, V> callback;


    /**
     * 在自己后面的wrapper，如果没有，自己就是末尾；
     * 如果有一个，就是串行；
     * 如果有多个，有几个就需要开几个线程</p>
     * -------2
     * 1
     * -------3
     * 如1后面有2、3
     */
    private List<WorkerWrapper<?, ?>> nextWrappers;

    /**
     * 依赖的wrappers，有2种情况，
     * 1:必须依赖的全部完成后，才能执行自己
     * 2:依赖的任何一个、多个完成了，就可以执行自己
     * 通过must字段来控制是否依赖项必须完成
     * 1
     * -------3
     * 2
     * 1、2执行完毕后才能执行3
     */
    private List<DependWrapper> dependWrappers;

    /**
     * 标记该事件是否已经被处理过了，譬如已经超时返回false了，后续rpc又收到返回值了，则不再二次回调
     * 经试验,volatile并不能保证"同一毫秒"内,多线程对该值的修改和拉取
     * <p>
     * 1-finish, 2-error, 3-working
     */
    private AtomicInteger state = new AtomicInteger(0);

    /**
     * 该map存放所有wrapper的id和wrapper映射
     */
    private Map<String, WorkerWrapper> forParamUseWrappers;

    /**
     * 也是个钩子变量，用来存临时的结果
     */
    private volatile WorkResult<V> workResult = WorkResult.defaultResult();

    /**
     * 是否在执行自己前，去校验nextWrapper的执行结果<p>
     * 1   4
     * -------3
     * 2
     * 如这种在4执行前，可能3已经执行完毕了（被2执行完后触发的），那么4就没必要执行了。
     * 注意，该属性仅在nextWrapper数量<=1时有效，>1时的情况是不存在的
     */
    private volatile boolean needCheckNextWrapperResult = true;

    private static final int FINISH = 1;
    private static final int ERROR = 2;
    private static final int WORKING = 3;
    private static final int INIT = 0;


    public WorkerWrapper(String id, String name, T param, IWorker<T, V> worker, ICallBack<T, V> callback) {
        if (worker == null) {
            throw new NullPointerException("async.worker is null");
        }
        this.id = id;
        this.param = param;
        this.name = name;
        this.iWorker = worker;
        //允许不设置回调
        if (callback == null) {
            /**设置默认的回调函数*/
            callback = new DefaultCallback<>();
        }
        this.callback = callback;
    }

    public void work(ThreadPoolExecutor threadPoolExecutor, long timeout, Map<String, WorkerWrapper> forParamUseWrappers) {
        try {
            work(threadPoolExecutor, null, timeout, forParamUseWrappers);
        } catch (Exception ex) {

            ex.printStackTrace();
        }

    }


    /**
     * 开始执行work
     *
     * @param threadPoolExecutor
     * @param fromWrapper         上游wrapper
     * @param timeout
     * @param forParamUseWrappers wrapper集合
     * @return: void
     * @author: xl
     * @date: 2021/11/27
     **/
    public void work(ThreadPoolExecutor threadPoolExecutor, WorkerWrapper fromWrapper,
                     long timeout, Map<String, WorkerWrapper> forParamUseWrappers) {
        this.forParamUseWrappers = forParamUseWrappers;
        /**存储当前任务*/
        forParamUseWrappers.put(id, this);
        long now = SystemClock.now();
        /**work调用链已经超时啦，就快速失败，进行下一个*/
        if (timeout <= 0) {
            System.out.println("=== 超时，快速失败 ===");
            fastFail(INIT, null);
            beginNextWorker(threadPoolExecutor, now, timeout);
            return;
        }

        /**
         * 如果自己已经执行过了。可能有多个依赖，其中的一个依赖已经执行完了，并且自己也已开始执行或执行完毕。
         当另一个依赖执行完毕，又进来该方法时，就不重复处理了*/
//        if (getState() == FINISH || getState() == ERROR) {
//            System.out.println(">>>>>>>> 当前任务状态等于完成或失败 : " + getState() + " <<<<<<<<");
//            beginNextWorker(threadPoolExecutor, now, timeout);
//            return;
//        }
        /**如果在执行前 需要校验一下nextWrapper的状态*/
        if (needCheckNextWrapperResult) {
            /**如果链上已经有任务执行完成啦，自己就不用执行啦,执行下一个*/
            if (!checkNextWrapperResult()) {
                System.out.println("=== 快速失败 ===");
                fastFail(INIT, null);
                beginNextWorker(threadPoolExecutor, now, timeout);
                return;
            }
        }

        /**如果没有任何依赖，说明自己就是第一批执行的*/
        if (dependWrappers == null || dependWrappers.size() == 0) {
            System.out.println("=== 没有任何依赖 继续执行 time:" + SystemClock.now() + " " + Thread.currentThread().getName() + " ===");
            //执行当前worker，获取结果
            execute();
            //执行下一个任务
            beginNextWorker(threadPoolExecutor, now, timeout);
            return;
        }

        /**
         *  如果前方有依赖，存在两种情况：
         *  1,前面只有一个wrapper，即 A-> B
         *  2,前面有多个wrapper，即 A C D -> B ,需要A C D都完成啦才能轮到B。
         *      但是无论是A执行完 还是C D 执行完 都会去唤醒B，
         *      所以需要B来做判断，必须B来判断，必须是ACD全部执行完，自己才能执行
         *
         * */
        if (dependWrappers.size() == 1) {
            System.out.println("===  存在依赖只有一个job time:" + SystemClock.now() + " 当前线程：" + Thread.currentThread().getName() + " === ");
            /**有一个依赖时*/
            doDependsOneJob(fromWrapper);
            /**前面的任务正常执行完毕，执行自己的任务*/
            execute();
            /**执行下一个任务*/
            beginNextWorker(threadPoolExecutor, now, timeout);
        } else {
            /**有多个依赖时*/
            doDependsJobs(threadPoolExecutor, dependWrappers, fromWrapper, now, timeout);
        }
    }

    /**
     * 校验调用链是否已全部执行完成
     */
    private boolean checkNextWrapperResult() {
        if (nextWrappers == null || nextWrappers.size() != 1) {
            return getState() == INIT;
        }
        WorkerWrapper nextWrapper = nextWrappers.get(0);
        boolean state = nextWrapper.getState() == INIT;
        /**继续校验下一个调用链的状态*/
        return state && nextWrapper.checkNextWrapperResult();
    }

    /**
     * 执行自己的job.具体的执行是在另一个线程里
     * 但判断阻塞超时是在work线程
     */
    private void execute() {
        //阻塞取结果
        System.out.println("=== time:" + SystemClock.now() + " 阻塞取结果 " + Thread.currentThread().getName() + " ===");
        workResult = workerDoJob();
    }

    /**
     * 具体单个worker执行任务
     */
    public WorkResult<V> workerDoJob() {

        /**避免重复执行(如果执行完成之后，workerresult 不能等于 default value)*/
        if (!checkIsNullResult()) {
            return workResult;
        }
        try {
            /**更新任务状态为 working，如果已经不是INIT 状态，说明正在被执行或执行完成，这一部很重要，可以保证任务不重复执行*/
            if (!compareAndSetState(INIT, WORKING)) {
                System.out.println("=== 当前任务已经执行完成或 正在执行中 ===");
                return workResult;
            }
            /**记录开始执行任务，调用回调方法*/
            callback.begin();

            /**具体执行我们的业务*/
            V resultValue = iWorker.action(param, forParamUseWrappers);

            /**更新任务状态为 success，如果状态不是working，说明被其他地方改变啦*/
            if (!compareAndSetState(WORKING, FINISH)) {
                System.out.println("=== 当前任务已经执行完成或 正在执行中 ===");
                return workResult;
            }

            workResult.setResultState(ResultState.SUCCESS);
            workResult.setResult(resultValue);
            /**回调成功*/
            callback.call(true, param, workResult);
            return workResult;
        } catch (Exception ex) {
            /**避免重复回调*/
            if (!checkIsNullResult()) {
                return workResult;
            }

            fastFail(WORKING, null);
            return workResult;
        }
    }


    /**
     * 进行下一个任务
     *
     * @param now                当前时间
     * @param threadPoolExecutor 线程池
     * @param timeout            超时时间
     */
    private void beginNextWorker(ThreadPoolExecutor threadPoolExecutor, long now, long timeout) {
        /**耗时时间*/
        long costtime = SystemClock.now() - now;
        if (nextWrappers == null) {
            return;
        }
        /**调用链只有一个时，使用当前线程执行*/
        if (nextWrappers.size() == 1) {
            nextWrappers.get(0).work(threadPoolExecutor, WorkerWrapper.this,
                    timeout - costtime, forParamUseWrappers);
            return;
        }
        /**当调用链有多个时，进行多线程并发执行*/
        CompletableFuture[] futures = new CompletableFuture[nextWrappers.size()];
        for (int i = 0; i < nextWrappers.size(); i++) {
            int finalI = i;
            futures[i] = CompletableFuture.runAsync(() -> nextWrappers.get(finalI).work(threadPoolExecutor, WorkerWrapper.this
                    , timeout - costtime, forParamUseWrappers), threadPoolExecutor);
        }
        try {
            CompletableFuture.allOf(futures).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 快速失败
     */
    public boolean fastFail(int newVal, Exception e) {
        /**试图将当前wrapper  从 newVal 值改到 error*/
        if (!compareAndSetState(newVal, ERROR)) {
            return false;
        }
        /**处理尚未处理过的结果*/
        if (checkIsNullResult()) {
            if (e == null) {
                workResult = defaultResult();
            } else {
                workResult = defaultExResult(e);
            }
        }
        callback.call(false, param, workResult);
        return true;
    }


    /**
     * 处理只有一个依赖的任务
     */
    private void doDependsOneJob(WorkerWrapper dependWrapper) {
        System.out.println("=== 当前 time:" + SystemClock.now() + " dependWrapper 状态: " + dependWrapper.getWorkResult().getResultState() + "");
        /**校验依赖的 workerwrapper 是否执行完毕*/
        if (ResultState.TIMEOUT == dependWrapper.getWorkResult().getResultState()) {
            workResult = defaultResult();
            fastFail(INIT, null);
        } else if (ResultState.EXCEPTION == dependWrapper.getWorkResult().getResultState()) {
            workResult = defaultExResult(dependWrapper.getWorkResult().getEx());
            fastFail(INIT, null);
        }
    }


    /**
     * 处理多个依赖job,此处会有并发 操作 例如 ：
     * 0
     * 两个依赖任务同时并发操作
     * 1
     *
     * @param threadPoolExecutor
     * @param now
     * @param dependWrappers
     * @param fromWrapper
     * @param timeout
     */
    private synchronized void doDependsJobs(ThreadPoolExecutor threadPoolExecutor, List<DependWrapper> dependWrappers,
                                            WorkerWrapper fromWrapper, long now, long timeout) {
        System.out.println("===  存在多个依赖job time:" + SystemClock.now() + " 当前线程：" + Thread.currentThread().getName() + " ===");

        boolean nowDepandIsMust = false;
        /**创建必须完成上有的wrapper集合*/
        Set<DependWrapper> mustWrapper = new HashSet<>();
        for (DependWrapper dependWrapper : dependWrappers) {
            /**校验当前依赖项是否是强依赖的*/
            if (dependWrapper.isMust()) {
                mustWrapper.add(dependWrapper);
            }
            /**校验依赖项是否与上游相等*/
            if (dependWrapper.getDependWrapper().equals(fromWrapper)) {
                nowDepandIsMust = dependWrapper.isMust();
            }
        }

        /**如果都不是不必须依赖的条件，到这里 只执行自己*/
        if (mustWrapper.size() == 0) {
            if (ResultState.TIMEOUT == fromWrapper.getWorkResult().getResultState()) {
                fastFail(INIT, null);
            } else {
                System.out.println("=== 非必须依赖条件 ，只执行自己 ===");
                execute();
            }
            /**执行下一个*/
            beginNextWorker(threadPoolExecutor, now, timeout);
            return;
        }

        /**todo 如果存在需要必须完成的，且 fromWrapper 不是必须的，就什么都不做*/
        if (!nowDepandIsMust) {
            return;
        }

        /**如果fromwrapper 是必须依赖项*/
        boolean existNoFinish = false;
        boolean hasError = false;

        /**判断所依赖项的执行结果状态，如果失败，不走自己的action，直接break*/
        for (DependWrapper dependWrapper : dependWrappers) {
            WorkerWrapper workerWrapper = dependWrapper.getDependWrapper();
            WorkResult tempWorkResult = workerWrapper.getWorkResult();
            /**校验状态 为 init  或 working，说明他依赖的某个任务还没有执行完毕
             * 他会等待最后一个任务执行完成走进来，是一个一致性操作。
             * */
            if (workerWrapper.getState() == INIT || workerWrapper.getState() == WORKING) {
                existNoFinish = true;
                break;
            }
            /**校验任务执行结果状态*/
            if (ResultState.TIMEOUT == tempWorkResult.getResultState()) {
                workResult = defaultResult();
                hasError = true;
                break;
            }
            if (ResultState.EXCEPTION == tempWorkResult.getResultState()) {
                workResult = defaultExResult(tempWorkResult.getEx());
                hasError = true;
                break;
            }
        }

        /**校验是否有失败的任务*/
        if (hasError) {
            fastFail(INIT, null);
            beginNextWorker(threadPoolExecutor, now, timeout);
            return;
        }
        /**如果上游都没有失败，都已经全部完成，处理自己的任务*/
        if (!existNoFinish) {
            execute();
            beginNextWorker(threadPoolExecutor, now, timeout);
        }
    }


    /**
     * 校验任务结果是否执行完毕
     */
    private boolean checkIsNullResult() {
        return ResultState.DEFAULT == workResult.getResultState();
    }


    /**
     * 更新任务状态
     *
     * @param expect 旧值
     * @param update 更新的值
     */
    private boolean compareAndSetState(int expect, int update) {
        return this.state.compareAndSet(expect, update);
    }

    /**
     * 设置默认值
     */
    private WorkResult<V> defaultResult() {
        workResult.setResultState(ResultState.TIMEOUT);
        workResult.setResult(iWorker.defaultValue());
        return workResult;
    }

    /**
     * 设置异常默认值
     */
    private WorkResult<V> defaultExResult(Exception ex) {
        workResult.setResultState(ResultState.EXCEPTION);
        workResult.setResult(iWorker.defaultValue());
        workResult.setEx(ex);
        return workResult;
    }


    public WorkResult<V> getWorkResult() {
        return workResult;
    }

    public void setWorkResult(WorkResult<V> workResult) {
        this.workResult = workResult;
    }


    public int getState() {
        return state.get();
    }


    public void setNeedCheckNextWrapperResult(boolean needCheckNextWrapperResult) {
        this.needCheckNextWrapperResult = needCheckNextWrapperResult;
    }

    /**
     * 添加调用链
     */
    private void addNext(WorkerWrapper<?, ?> workerWrapper) {
        if (nextWrappers == null) {
            nextWrappers = new ArrayList<>();


        }
        /**校验避免重复添加*/
        for (WorkerWrapper<?, ?> nextWrapper : nextWrappers) {
            if (workerWrapper.equals(nextWrapper)) {
                return;
            }
        }
        nextWrappers.add(workerWrapper);
    }


    private void addDepend(WorkerWrapper<?, ?> workerWrapper, boolean must) {
        addDepend(new DependWrapper(workerWrapper, must));
    }

    /**
     * 添加依赖项
     */
    private void addDepend(DependWrapper dependWrapper) {
        if (dependWrappers == null) {
            dependWrappers = new ArrayList<>();
        }
        /**校验避免重复添加*/
        for (DependWrapper wrapper : dependWrappers) {
            if (wrapper.equals(dependWrapper)) {
                return;
            }
        }
        dependWrappers.add(dependWrapper);
    }

    public List<WorkerWrapper<?, ?>> getNextWrappers() {
        return nextWrappers;
    }

    public void setNextWrappers(List<WorkerWrapper<?, ?>> nextWrappers) {
        this.nextWrappers = nextWrappers;
    }


    /**
     * 总控制台超时，停止所有任务
     */
    public void stopNow() {
        if (getState() == INIT || getState() == WORKING) {
            System.err.println("=== stop 当前任务 workerName：" + name + " 状态：" + getState() + " ===");
            fastFail(getState(), null);
        }
    }


    public T getParam() {
        return param;
    }

    public void setParam(T param) {
        this.param = param;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * 创建构造模式
     */
    public static class Builder<W, C> {

        /**
         * 该wrapper的唯一标识[实际业务中可以使用全局唯一业务ID]
         */
        private String id = UUID.randomUUID().toString();

        /**
         * worker的param
         */
        private W param;

        /**
         * worker上的name
         */
        private String name;

        /**
         * 具体要执行的worker
         */
        private IWorker<W, C> worker;
        /**
         * 具体要执行的回调
         */
        private ICallBack<W, C> callback;
        /**
         * 自己后面的所有任务
         */
        private List<WorkerWrapper<?, ?>> nextWrappers;
        /**
         * 自己依赖的所有
         */
        private List<DependWrapper> dependWrappers;
        /**
         * 存储 强依赖于 自己的 wrapper 集合
         */
        private Set<WorkerWrapper<?, ?>> selfIsMustSet;

        /**
         * 是否需要校验调用链的结果状态 默认需要
         */
        private boolean needCheckNextWrapperResult = true;


        public Builder<W, C> worker(IWorker<W, C> worker) {
            this.worker = worker;
            return this;
        }

        public Builder<W, C> param(W w) {
            this.param = w;
            return this;
        }

        public Builder<W, C> id(String id) {
            if (id != null) {
                this.id = id;
            }
            return this;
        }

        public Builder<W, C> name(String name) {
            if (name != null) {
                this.name = name;
            }
            return this;
        }

        public Builder<W, C> needCheckNextWrapperResult(boolean needCheckNextWrapperResult) {
            this.needCheckNextWrapperResult = needCheckNextWrapperResult;
            return this;
        }

        public Builder<W, C> callback(ICallBack<W, C> callback) {
            this.callback = callback;
            return this;
        }

        /**
         * 处理依赖项
         */
        public Builder<W, C> depend(WorkerWrapper<?, ?>... workerWrappers) {

            if (workerWrappers == null) {
                return this;
            }
            for (WorkerWrapper<?, ?> workerWrapper : workerWrappers) {
                depend(workerWrapper);
            }
            return this;
        }

        public Builder<W, C> depend(WorkerWrapper<?, ?> wrapper) {
            return depend(wrapper, true);
        }

        /**
         * 处理依赖项
         *
         * @param isMust  是否强依赖
         * @param wrapper
         */
        public Builder<W, C> depend(WorkerWrapper<?, ?> wrapper, boolean isMust) {
            if (wrapper == null) {
                return this;
            }
            DependWrapper dependWrapper = new DependWrapper(wrapper, isMust);
            if (dependWrappers == null) {
                dependWrappers = new ArrayList<>();
            }
            dependWrappers.add(dependWrapper);
            return this;
        }


        public Builder<W, C> next(WorkerWrapper<?, ?>... wrappers) {
            if (wrappers == null) {
                return this;
            }
            for (WorkerWrapper<?, ?> wrapper : wrappers) {
                next(wrapper);
            }
            return this;
        }

        public Builder<W, C> next(WorkerWrapper<?, ?> wrapper) {
            return next(wrapper, true);
        }

        /**
         * 添加调用链
         */
        public Builder<W, C> next(WorkerWrapper<?, ?> wrapper, boolean selfIsMust) {
            if (nextWrappers == null) {
                nextWrappers = new ArrayList<>();
            }
            nextWrappers.add(wrapper);

            /**校验是否强依赖自己*/
            if (selfIsMust) {
                if (selfIsMustSet == null) {
                    selfIsMustSet = new HashSet<>();
                }
                selfIsMustSet.add(wrapper);
            }
            return this;
        }

        public WorkerWrapper<W, C> build() {
            WorkerWrapper<W, C> wrapper = new WorkerWrapper<>(id, name, param, worker, callback);
            wrapper.setNeedCheckNextWrapperResult(needCheckNextWrapperResult);
            if (dependWrappers != null) {
                for (DependWrapper dependWrapper : dependWrappers) {
                    dependWrapper.getDependWrapper().addNext(wrapper);
                    wrapper.addDepend(dependWrapper);
                }
            }
            if (nextWrappers != null) {
                for (WorkerWrapper<?, ?> nextWrapper : nextWrappers) {
                    boolean isMust = false;
                    if (selfIsMustSet != null && selfIsMustSet.contains(nextWrapper)) {
                        isMust = true;
                    }
                    nextWrapper.addDepend(wrapper, isMust);
                    wrapper.addNext(nextWrapper);
                }
            }
            return wrapper;
        }
    }
}
