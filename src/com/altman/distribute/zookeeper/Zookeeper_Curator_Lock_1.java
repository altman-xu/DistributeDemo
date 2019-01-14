package com.altman.distribute.zookeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 单应用跑没问题，多应用跑不能实现锁
 * @author xuzhihua
 * @date 2018/12/22 5:01 PM
 */
public class Zookeeper_Curator_Lock_1 {

    static ReentrantLock reentrantLock = new ReentrantLock();
    static int count = 10;

    public static void genarNo() {
        try {
            reentrantLock.lock();
            count --;
            System.out.println(count);
        } finally {
            reentrantLock.unlock();
        }
    }

    public static void main(String[] args) throws Exception {
        final CountDownLatch countdown = new CountDownLatch(1);
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countdown.await();
                        genarNo();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {

                    }
                }
            }, "t" + i).start();
        }
        Thread.sleep(1000);
        countdown.countDown();
    }

}
