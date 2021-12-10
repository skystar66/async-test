package com.async.pst.service;

import com.async.pst.woker.ParWorker;
import com.async.pst.woker.ParWorker1;
import com.async.pst.woker.ParWorker2;
import com.async.pst.woker.ParWorker3;
import com.xuliang.framework.async.excuter.Async;
import com.xuliang.framework.async.excuter.timer.SystemClock;
import com.xuliang.framework.async.wrapper.WorkerWrapper;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class AsyncService {


    public static void main(String[] args) {
        try {
            new AsyncService().result();

        } catch (Exception e) {
            {
            }
        }
    }


    public String result() throws ExecutionException, InterruptedException {

        /**任务执行策略：
         *    1
         * 0      3
         *    2
         * */


        ParWorker parWorker0 = new ParWorker();
        ParWorker1 parWorker1 = new ParWorker1();

        ParWorker2 parWorker2 = new ParWorker2();

        ParWorker3 parWorker3 = new ParWorker3();

        WorkerWrapper<String, String> wrapper3 = new WorkerWrapper.Builder<String, String>()
                .callback(parWorker3)
                .worker(parWorker3)
                .param("3")
                .name("wrapper3-worker")
                .build();


        WorkerWrapper<String, String> wrapper1 = new WorkerWrapper.Builder<String, String>()
                .callback(parWorker1)
                .worker(parWorker1)
                .param("1")
                .next(wrapper3)
                .name("wrapper1-worker")
                .build();

        WorkerWrapper<String, String> wrapper2 = new WorkerWrapper.Builder<String, String>()
                .callback(parWorker2)
                .worker(parWorker2)
                .param("2")
                .next(wrapper3)
                .name("wrapper2-worker")
                .build();


        WorkerWrapper<String, String> wrapper = new WorkerWrapper.Builder<String, String>()
                .callback(parWorker0)
                .worker(parWorker0)
                .param("0")
                .next(wrapper1, wrapper2)
                .name("wrapper0-worker")
                .build();


        long now = SystemClock.now();
        Async.beginWork(4500, wrapper);
        System.out.println("cost time : " + (SystemClock.now() - now) + "ms");
//        Async.shutDown();
        return wrapper.getWorkResult().getResult();

    }


}
