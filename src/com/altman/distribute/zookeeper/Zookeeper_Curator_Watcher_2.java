package com.altman.distribute.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author xuzhihua
 * @date 2018/12/22 4:09 PM
 */
public class Zookeeper_Curator_Watcher_2 {

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

        // 4. 建立一个 PathChildrenCache 缓存, 第三个参数为是否接受节点数据内容，如果为false则不接受
        PathChildrenCache cache = new PathChildrenCache(cf, "/super", true);
        // 5. 在初始化的时候进行缓存监听
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework cf, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("CHILD_ADDED: " + event.getData().getPath());
                        System.out.println("CHILD_ADDED: " + new String(event.getData().getData()));
                        break;
                    case CHILD_UPDATED:
                        System.out.println("CHILD_UPDATED: " + event.getData().getPath());
                        System.out.println("CHILD_UPDATED: " + new String(event.getData().getData()));
                        break;
                    case CHILD_REMOVED:
                        System.out.println("CHILD_REMOVED: " + event.getData().getPath());
                        System.out.println("CHILD_REMOVED: " + new String(event.getData().getData()));
                        break;
                    default:
                        break;
                }
            }
        });

        // 创建本身节点不会被监听到
//        Thread.sleep(1000);
//        cf.delete().deletingChildrenIfNeeded().forPath("/super");
//        Thread.sleep(1000);
//        cf.create().forPath("/super", "init data".getBytes());

        // 添加子节点
        Thread.sleep(1000);
        cf.create().forPath("/super/c1", "c1 data".getBytes());
        Thread.sleep(1000);
        cf.create().forPath("/super/c2", "c2 data".getBytes());

        // 修改子节点
        Thread.sleep(1000);
        cf.setData().forPath("/super/c1", "c1 modify".getBytes());
        Thread.sleep(1000);
        cf.setData().forPath("/super/c2", "c2 modify".getBytes());

        // 删除子节点
        Thread.sleep(1000);
        cf.delete().forPath("/super/c2");

        // 删除本身节点
        Thread.sleep(1000);
        cf.delete().deletingChildrenIfNeeded().forPath("/super");

//        Thread.sleep(10000);



    }
}