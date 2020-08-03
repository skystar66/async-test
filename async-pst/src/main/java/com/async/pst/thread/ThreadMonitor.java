package com.async.pst.thread;

import com.xuliang.framework.async.excuter.Async;

public class ThreadMonitor implements Runnable {


    @Override
    public void run() {

        while (true) {

            try {
                /**每10秒打印一下线程数量*/
                Thread.sleep(10000);
            } catch (Exception ex) {

            }
            System.out.println(Async.getThreadCount());

        }


    }
}



