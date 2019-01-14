package com.altman.distribute.zookeeper;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author xuzhihua
 * @date 2018/12/18 10:18 AM
 */
public class Zookeeper_OriginalAPI_1 {

    /* zookeeper 服务器地址 */
    static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* session 超时时间 */
    static final int SESSION_OUTTIME = 5000; //ms

    /* 阻塞程序执行，用于等待 zookeeper 连接成功，发送成功信号 */
    static final CountDownLatch connectedSemaphore = new CountDownLatch(1);
    static final CountDownLatch connectedSemaphore2 = new CountDownLatch(1);

    public static void main(String[] args) throws Exception{
        ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, new Watcher(){
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 获取事件的状态
                Event.KeeperState keeperState = watchedEvent.getState();
                Event.EventType eventType = watchedEvent.getType();
                // 如果是建立连接
                if (Event.KeeperState.SyncConnected == keeperState) {
                    if (Event.EventType.None == eventType) {
                        // 如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                        connectedSemaphore.countDown();
                        System.out.println("zk 建立连接 成功");
                    }
                }
            }
        });

        // 进行阻塞
        connectedSemaphore.await();

        // zk连接成功后，开始执行
        System.out.println("zk 开始执行 ");

        // 创建父结点 先判断节点是否存在
        if (zk.exists("/testRoot", false) == null) {
            zk.create("/testRoot", "testRoot".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println("创建后，输出节点 /testRoot 值: " + new String(zk.getData("/testRoot", false, null)));
        // 创建子节点
        if (zk.exists("/testRoot/childrenNood1", false) == null) {
            zk.create("/testRoot/childrenNood1", "childrenNood1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println("创建后，输出节点 /testRoot/childrenNood1 值: " + new String(zk.getData("/testRoot/childrenNood1", false, null)));


        // 修改节点数据
        zk.setData("/testRoot/childrenNood1", "childrenNood1ModifyData".getBytes(), -1);
        System.out.println("修改后，输出节点 /testRoot/childrenNood1 值: " + new String(zk.getData("/testRoot/childrenNood1", false, null)));

        // 获取节点信息
        byte[] data = zk.getData("/testRoot", false, null);
        System.out.println(new String(data));
        List<String> list = zk.getChildren("/testRoot", false);
        for (String path : list) {
            String realPath = "/testRoot/" + path;
            System.out.println("遍历节点 /testRoot，输出子节点值 " + realPath + " : " + new String(zk.getData(realPath, false, null)));
        }

//        zk.create("testRoot/childrenNood2", "childrenNood2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 删除节点
        zk.delete("/testRoot/childrenNood1", -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int i, String s, Object o) {
                System.out.println(i);
                System.out.println(s);
                System.out.println(o);
            }
        }, "childrenNood1");

        // 删除节点
        zk.delete("/testRoot", -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int i, String s, Object o) {
                System.out.println(i);
                System.out.println(s);
                System.out.println(o);
            }
        }, "testRoot");



        zk.close();

    }

}
