package com.xuliang.framework.async.callback;


import com.xuliang.framework.async.wrapper.WorkerWrapper;

import java.util.Map;

/**
 * 每个最小执行单元需要实现该接口</p>
 *
 * @author xuliang wrote on 2020-07-28.
 */
@FunctionalInterface
public interface IWorker<T, V> {


    /**
     * 超时、异常时，返回的默认值
     *
     * @return 默认值
     */
    default V defaultValue() {
        return null;
    }

    /**
     * 在这里做耗时操作，如rpc请求、IO等
     *
     * @param object      object
     * @param allWrappers 任务包装
     */
    public V action(T object, Map<String, WorkerWrapper> allWrappers);

}
