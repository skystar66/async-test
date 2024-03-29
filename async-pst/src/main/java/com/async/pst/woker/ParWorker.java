package com.async.pst.woker;

import com.xuliang.framework.async.callback.ICallBack;
import com.xuliang.framework.async.callback.IWorker;
import com.xuliang.framework.async.worker.WorkResult;
import com.xuliang.framework.async.wrapper.WorkerWrapper;

import java.util.Map;

/**
 * @author xuliang
 * */
public class ParWorker implements IWorker<String, String>, ICallBack<String, String> {


    @Override
    public String action(String object, Map<String, WorkerWrapper> allWrappers) {
        try {
            Thread.sleep(1);
//            throw new RuntimeException("故意打出异常");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return object + " from 0";
    }


    @Override
    public void call(boolean success, String param, WorkResult<String> workResult) {
        if (success) {
            System.out.println("callback worker0 success--" + workResult.getResult()
                    + "-threadName:" + Thread.currentThread().getName()+"-param:"+param);
        } else {
            System.err.println("callback worker0 failure--" + workResult.getResult() + "-- ex " + workResult.getEx()
                    + "-threadName:" + Thread.currentThread().getName()+"-param:"+param);
        }
    }

    @Override
    public void begin() {

    }

    @Override
    public String defaultValue() {
        return "worker0--default";
    }


}
