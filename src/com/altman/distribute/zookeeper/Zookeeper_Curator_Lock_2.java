package com.altman.distribute.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * curator 实现分布式锁
 * @author xuzhihua
 * @date 2018/12/22 5:01 PM
 */
public class Zookeeper_Curator_Lock_2 {
    /* zookeeper 服务器地址 */
    static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* session 超时时间 */
    static final int SESSION_OUTTIME = 5000; //ms

    public static CuratorFramework createCuratorFramework() {
        // 1. 重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        // 2. 通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();
        return cf;
    }

    public static void main(String[] args) throws Exception {
        final CountDownLatch countdown = new CountDownLatch(1);
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    CuratorFramework cf = createCuratorFramework();
                    cf.start();
                    final InterProcessMutex lock = new InterProcessMutex(cf, "/superLock");
//                    final ReentrantLock reentrantLock = new ReentrantLock();
                    try {
                        countdown.await();
                        lock.acquire();
//                        reentrantLock.lock();
                        System.out.println(Thread.currentThread().getName() + " 执行业务逻辑...");
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            lock.release();
//                            reentrantLock.unlock();
                        } catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                }
            }, "t" + i).start();
        }
        Thread.sleep(2000);
        countdown.countDown();

    }

}
