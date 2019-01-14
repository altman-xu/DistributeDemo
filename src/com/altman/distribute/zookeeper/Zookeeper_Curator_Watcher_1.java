package com.altman.distribute.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author xuzhihua
 * @date 2018/12/22 4:07 PM
 */
public class Zookeeper_Curator_Watcher_1 {

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
        // 3. 建立连接
        cf.start();

        // 4. 建立一个 cache 缓存
        final NodeCache cache = new NodeCache(cf, "/super", false);
        cache.start(true);

        cache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("路径为: " + cache.getCurrentData().getPath());
                System.out.println("数据为: " + new String(cache.getCurrentData().getData()));
                System.out.println("状态为: " + cache.getCurrentData().getStat());
                System.out.println("--------------------------");
            }
        });

        Thread.sleep(1000);
        cf.delete().deletingChildrenIfNeeded().forPath("/super");
        Thread.sleep(1000);
        cf.create().forPath("/super", "123".getBytes());

        Thread.sleep(1000);
        cf.setData().forPath("/super", "456".getBytes());

        Thread.sleep(1000);
        cf.delete().deletingChildrenIfNeeded().forPath("/super");

        Thread.sleep(10000);

    }

}
