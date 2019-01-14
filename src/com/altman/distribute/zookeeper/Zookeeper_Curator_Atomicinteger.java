package com.altman.distribute.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

/**
 * @author xuzhihua
 * @date 2018/12/22 5:26 PM
 */
public class Zookeeper_Curator_Atomicinteger {
    /* zookeeper 服务器地址 */
    static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* session 超时时间 */
    static final int SESSION_OUTTIME = 5000; //ms

    public static void main(String[] args) throws Exception {
        // 1. 重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        // 2. 通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();
        // 3. 开启连接
        cf.start();
//        if (cf.checkExists().forPath("/superAtomicInteger") != null){
//            cf.delete().deletingChildrenIfNeeded().forPath("/superAtomicInteger");
//        }
        // 4. 使用 DistributedAtomicInteger
        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(cf, "/superAtomicInteger", new RetryNTimes(3, 1000));

        atomicInteger.increment();
//        atomicInteger.forceSet(0);
        AtomicValue<Integer> value = atomicInteger.get();
        System.out.println(value.succeeded());
        System.out.println(value.postValue());  // 最新值
        System.out.println(value.preValue());   // 原始值

    }


}
