package com.altman.distribute.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author xuzhihua
 * @date 2018/12/21 3:38 PM
 */
public class Zookeeper_Curator_Base {

    /* zookeeper 服务器地址 */
    static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* session 超时时间 */
    static final int SESSION_OUTTIME = 5000; //ms

    public static void main(String[] args) throws Exception {

        // 1. 重试策略：初试时间为1s，重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        // 2. 通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();
        // 3. 开启连接
        cf.start();

        // 4. 建立节点，自动节点类型(不加 withMode 默认为持久型节点) 路径 数据内容
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/super/c1", "c1".getBytes());
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/super/c2", "c2".getBytes());

        // 5. 读取节点数据
        System.out.println("读取节点数据: " + new String(cf.getData().forPath("/super/c1")));

        // 6. 修改节点数据
        cf.setData().forPath("/super/c1", "c1 modify".getBytes());
        System.out.println("读取修改后的节点数据: " + new String(cf.getData().forPath("/super/c1")));

        // 7.读取子节点 getChildren 方法 和判断节点是否存在 checkExists 方法
        List<String> list = cf.getChildren().forPath("/super");
        for (String p : list) {
            System.out.println("读取子节点路径: " + p);
        }
        Stat stat = cf.checkExists().forPath("/super/c3_non");
        System.out.println("检查节点是否存在: " + stat);

        // 绑定回调函数
        ExecutorService pool = Executors.newCachedThreadPool();
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("code: " + curatorEvent.getResultCode());
                        System.out.println("type: " + curatorEvent.getType());
                        System.out.println("Thread_callback: " + Thread.currentThread().getName());
                    }
                }, pool)
                .forPath("/super/c3", "c3 callback data".getBytes());
        System.out.println("Thread_main: " + Thread.currentThread().getName());

        Thread.sleep(2000);

        // 8. 递归删除节点
        cf.delete().guaranteed().deletingChildrenIfNeeded().forPath("/super");

        cf.close();
    }

}
