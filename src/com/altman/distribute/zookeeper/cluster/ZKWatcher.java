package com.altman.distribute.zookeeper.cluster;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * @author xuzhihua
 * @date 2018/12/21 11:13 AM
 */
public class ZKWatcher implements Watcher {

    private ZooKeeper zk = null;
    static final String PARENT_PATH = "/super";
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private List<String> cowaList = new CopyOnWriteArrayList<>();
    public static final String CONNECT_ADDR = "127.0.0.1:2181";
    public static final int SESSION_TIMEOUT = 30000;

    public ZKWatcher() throws Exception {
        zk = new ZooKeeper(CONNECT_ADDR, SESSION_TIMEOUT, this);
        System.out.println("开始连接 zk 服务器");
        connectedSemaphore.await();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        Event.KeeperState keeperState = watchedEvent.getState();
        Event.EventType eventType = watchedEvent.getType();

        String path = watchedEvent.getPath();
        System.out.println("受影响的 path:  " + path);

        if (Event.KeeperState.SyncConnected == keeperState) {
            // 成功连接服务器
            if (Event.EventType.None == eventType) {
                System.out.println("成功连接上 zk 服务器");
                connectedSemaphore.countDown();

                try {
                    if (this.zk.exists(PARENT_PATH, false) == null) {
                        this.zk.create(PARENT_PATH, "root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    List<String> paths = this.zk.getChildren(PARENT_PATH, true);
                    for (String p : paths) {
                        System.out.println(p);
                        this.zk.exists(PARENT_PATH + "/" + p, true);
                    }
                } catch (KeeperException | InterruptedException e){
                    e.printStackTrace();
                }
            }
            // 创建节点
            else if (Event.EventType.NodeCreated == eventType) {
                System.out.println("节点创建");
                try {
                    this.zk.exists(path, true);
                } catch (KeeperException | InterruptedException e){
                    e.printStackTrace();
                }
            }
            // 变更节点
            else if (Event.EventType.NodeDataChanged == eventType) {
                System.out.println("节点数据变更");
                try {
                    this.zk.exists(path, true);
                } catch (KeeperException | InterruptedException e){
                    e.printStackTrace();
                }
            }
            // 变更子节点
            else if (Event.EventType.NodeChildrenChanged == eventType) {
                System.out.println("子节点数据变更");
                try {
                    List<String> paths = this.zk.getChildren(path, true);
                    if (paths.size() >= cowaList.size()) {
                        paths.removeAll(cowaList);
                        for (String p : paths) {
                            this.zk.exists(path + "/" + p, true);
                            System.out.println("这个是新增的子节点:  " + path + "/" + p);
                        }
                        cowaList.addAll(paths);
                    } else {
                        cowaList = paths;
                    }
                    System.out.println("cowaList: " + cowaList.toString());
                    System.out.println("paths: " + paths.toString());
                } catch (KeeperException | InterruptedException e){
                    e.printStackTrace();
                }
            }
            // 删除节点
            else if (Event.EventType.NodeDeleted == eventType) {
                System.out.println("节点删除 " + path);
                try {
                    this.zk.exists(path, true);
                } catch (KeeperException | InterruptedException e){
                    e.printStackTrace();
                }
            }
            else ;
        }
        else if (Event.KeeperState.Disconnected == keeperState) {
            System.out.println("与 zk 服务器断开连接");
        }
        else if (Event.KeeperState.AuthFailed == keeperState) {
            System.out.println("权限检查失败");
        }
        else if (Event.KeeperState.Expired == keeperState) {
            System.out.println("会话失效");
        }
        else;
        System.out.println("--------------------------------------------");

    }
}
