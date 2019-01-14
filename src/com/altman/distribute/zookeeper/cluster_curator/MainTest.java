package com.altman.distribute.zookeeper.cluster_curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * 测试步骤1. 先启动 Client1 Client2， 在启动 MainTest 观察 Client1 Client2 输出，最后关闭 Client1 Client2
 * 测试步骤2. 先启动 Client1 Client2， 在启动 MainTest 观察 Client1 Client2 输出，关闭 Client1， 重新运行Client1，观察Client1输出，最后关闭 Client1 Client2
 * @author xuzhihua
 * @date 2018/12/22 5:52 PM
 */
public class MainTest {

    /* 父结点path */
    static final String PARENT_PATH = "/super";

    /* zookeeper 服务器地址 */
    static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* session 超时时间 */
    static final int SESSION_OUTTIME = 5000; //ms

    public static void main(String[] args) throws Exception {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .retryPolicy(retryPolicy)
                .build();
        cf.start();

        if (cf.checkExists().forPath(PARENT_PATH) == null) {
            cf.create().withMode(CreateMode.PERSISTENT).forPath(PARENT_PATH, "super init".getBytes());
        }

        if (cf.checkExists().forPath(PARENT_PATH + "/c1") == null) {
            Thread.sleep(1000);
            cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(PARENT_PATH + "/c1", "c1 data".getBytes());
        }
        if (cf.checkExists().forPath(PARENT_PATH + "/c2") == null) {
            Thread.sleep(1000);
            cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(PARENT_PATH + "/c2", "c2 data".getBytes());
        }

        Thread.sleep(1000);
        System.out.println(new String(cf.getData().forPath(PARENT_PATH + "/c1")));

        Thread.sleep(1000);
        cf.setData().forPath(PARENT_PATH + "/c2", "c2 modify data".getBytes());
        System.out.println(new String(cf.getData().forPath(PARENT_PATH + "/c2")));

        Thread.sleep(1000);
        cf.delete().forPath(PARENT_PATH + "/c1");
//        cf.delete().forPath(PARENT_PATH + "/c2");

    }
}
