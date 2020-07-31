package com.xuliang.framework.async.callback;


import com.xuliang.framework.async.worker.WorkResult;

/**
 * 默认回调类，如果不设置的话，会默认给这个回调
 *
 * @author xuliang
 */
public class DefaultCallback<T, V> implements ICallBack<T, V> {
    @Override
    public void begin() {

    }

    @Override
    public void call(boolean success, T param, WorkResult<V> workResult) {

    }

}
