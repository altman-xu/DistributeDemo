package com.altman.distribute.zookeeper.cluster;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * @author xuzhihua
 * @date 2018/12/21 11:18 AM
 */
public class MainTest {

    static final String CONNECT_ADDR = "127.0.0.1:2181";
    static final int SESSION_OUTTIME = 2000;
    static final CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                Event.KeeperState keeperState = watchedEvent.getState();
                Event.EventType eventType = watchedEvent.getType();
                if (Event.KeeperState.SyncConnected == keeperState) {
                    if (Event.EventType.None == eventType) {
                        System.out.println("zk 建立连接");
                        connectedSemaphore.countDown();
                    }
                }
            }
        });
        // 进行阻塞
        connectedSemaphore.await();

        if (zk.exists("/super/c1", false) == null) {
            zk.create("/super/c1", "c1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zk.exists("/super/c2", false) == null){
            zk.create("/super/c2", "c2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zk.create("/super/c3", "c3".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/super/c4", "c4".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zk.setData("/super/c1", "modify c1".getBytes(), -1);
        zk.setData("/super/c2", "modify c2".getBytes(), -1);

        byte[] data = zk.getData("/super/c2", false, null);
        System.out.println(data.toString());

        System.out.println(zk.exists("/super/c3", false));

//        zk.delete("/super/c1", -1);
//        zk.delete("/super/c2", -1);
        zk.delete("/super/c3", -1);
        zk.delete("/super/c4", -1);

        zk.close();
    }

}
