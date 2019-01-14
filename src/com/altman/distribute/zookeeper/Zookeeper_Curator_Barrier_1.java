package com.altman.distribute.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Random;

/**
 * Barrier 等待所有线程准备完毕，分布式同时开始运行， 同时退出运行
 * @author xuzhihua
 * @date 2018/12/22 5:35 PM
 */
public class Zookeeper_Curator_Barrier_1 {
    /* zookeeper 服务器地址 */
    static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* session 超时时间 */
    static final int SESSION_OUTTIME = 5000; //ms

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
                        CuratorFramework cf = CuratorFrameworkFactory.builder()
                                .connectString(CONNECT_ADDR)
                                .retryPolicy(retryPolicy)
                                .build();
                        cf.start();

                        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(cf, "/superBarrier", 5);
                        Thread.sleep(1000*(new Random()).nextInt(3));
                        System.out.println(Thread.currentThread().getName() + " 已经准备");
                        barrier.enter();    // 准备 await()
                        System.out.println("同时开始运行...");
                        Thread.sleep(1000*(new Random()).nextInt(3));
                        System.out.println(Thread.currentThread().getName() + " 运行完毕");
                        barrier.leave();
                        System.out.println("同时退出运行...");
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }, "t" + i).start();
        }
    }

}
