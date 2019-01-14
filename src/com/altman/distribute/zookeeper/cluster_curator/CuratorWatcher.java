package com.altman.distribute.zookeeper.cluster_curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @author xuzhihua
 * @date 2018/12/22 5:52 PM
 */
public class CuratorWatcher {

    /* 父结点path */
    static final String PARENT_PATH = "/super";

    /* zookeeper 服务器地址 */
    static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* session 超时时间 */
    static final int SESSION_OUTTIME = 5000; //ms

    public CuratorWatcher() throws Exception {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .retryPolicy(retryPolicy)
                .build();
        cf.start();

        if (cf.checkExists().forPath(PARENT_PATH) == null) {
            cf.create().withMode(CreateMode.PERSISTENT).forPath(PARENT_PATH, "super init".getBytes());
        }

        // 建立 PathChildCache 缓存
        PathChildrenCache cache = new PathChildrenCache(cf, PARENT_PATH, true);
        // 在初始化的时候就进行缓存监听
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

    }

}
