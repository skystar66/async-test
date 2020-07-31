package com.xuliang.framework.async.callback;


import com.xuliang.framework.async.worker.WorkResult;

/**
 * 每个执行单元执行完毕后，会回调该接口</p>
 * 需要监听执行结果的，实现该接口即可
 *
 * @author xuliang wrote on 2020-07-28.
 */
@FunctionalInterface
public interface ICallBack<T, V> {


    /**
     * 任务开始监听
     */

    default void begin() {

    }


    /**
     * 回调方法
     */
    public void call(boolean success, T param, WorkResult<V> workResult);

}
