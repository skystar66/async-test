package com.async.pst.woker;

import com.xuliang.framework.async.callback.ICallBack;
import com.xuliang.framework.async.callback.IWorker;
import com.xuliang.framework.async.worker.WorkResult;
import com.xuliang.framework.async.wrapper.WorkerWrapper;

import java.util.Map;

/**
 * @author xuliang
 * */
public class ParWorker3 implements IWorker<String, String>, ICallBack<String, String> {
    private long sleepTime = 1;

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public String action(String object, Map<String, WorkerWrapper> allWrappers) {
        try {
            Thread.sleep(sleepTime);
//            throw new RuntimeException("故意打出异常");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return object + " from 3";
    }


    @Override
    public void call(boolean success, String param, WorkResult<String> workResult) {
        if (success) {
            System.out.println("callback worker3 success--" + workResult.getResult()
                    + "-threadName:" + Thread.currentThread().getName()+"-param:"+param);
        } else {
            System.err.println("callback worker3 failure--" + workResult.getResult() + "-- ex " + workResult.getEx()
                    + "-threadName:" + Thread.currentThread().getName()+"-param:"+param);
        }
    }

    @Override
    public void begin() {

    }

    @Override
    public String defaultValue() {
        return "worker3--default";
    }


}
