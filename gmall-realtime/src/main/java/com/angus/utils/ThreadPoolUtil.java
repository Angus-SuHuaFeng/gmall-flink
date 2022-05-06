package com.angus.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/2 23:02
 * @description：
 */
public class ThreadPoolUtil {

    static ThreadPoolExecutor threadPoolExecutor = null;

    public ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPool() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class){
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(16,
                            32,
                            1L,
                            TimeUnit.MINUTES, new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }

//    public static void main(String[] args) {
//        ThreadPoolExecutor threadPool = getThreadPool();
//        for (int i = 0; i < 10; i++) {
//            int finalI = i;
//            threadPool.submit(new Runnable() {
//                @Override
//                public void run() {
//                    System.out.println(Thread.currentThread().getName() + ": " + finalI);
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            });
//        }
//    }
}
