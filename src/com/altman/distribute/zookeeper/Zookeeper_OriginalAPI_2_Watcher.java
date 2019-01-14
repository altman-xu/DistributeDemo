package com.altman.distribute.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xuzhihua
 * @date 2018/12/18 11:42 AM
 */
public class Zookeeper_OriginalAPI_2_Watcher implements Watcher {

    /* 定义院子变量 */
    AtomicInteger seq = new AtomicInteger();
    /* 定义 session 失效时间 */
    private static final int SESSION_TIMEOUT = 10000;
    /* zookeeper 服务器地址 */
    private static final String CONNECT_ADDR = "127.0.0.1:2181";
    /* zk 父路径设置 */
    private static final String PARENT_PATH = "/p";
    /* zk 子路径设置 */
    private static final String CHILDREN_PATH = "/p/c";
    /* 进入标识 */
    private static final String LOG_PREFIX_OF_MAIN = "【Main】";
    /* zk 变量 */
    private ZooKeeper zk = null;
    /* 用于等待 zookeeper 连接建立后，通知阻塞程序继续向下执行 */
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    /**
     * 创建 zk 连接
     * @param connectAddr
     * @param sessionTimeout
     */
    public void createConnect(String connectAddr, int sessionTimeout) {
        this.releaseConnection();
        try {
            // this 标识把当前对象进行传递到其中去(也就是在主函数里实例化的 new ZookeeperWathcer() 实例对象)
            zk = new ZooKeeper(connectAddr, sessionTimeout, this);
            System.out.println(LOG_PREFIX_OF_MAIN + " 开始连接 zk 服务器");
            connectedSemaphore.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放 zk 连接
     */
    public void releaseConnection(){
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建节点
     * @param path
     * @param data
     * @param needWatch
     * @return
     */
    public boolean createPath(String path, String data, boolean needWatch) {
        try {
            // 设置监控(由于 zookeeper 的监控都是一次性的，所以每次必须设置监控)
            this.zk.exists(path, needWatch);
            System.out.println(LOG_PREFIX_OF_MAIN + " 节点创建成功，Path: " +
                    this.zk.create(
                            path,
                            data.getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT
                    ) + ", contant: " + data);
        } catch (Exception e) {
            e.printStackTrace();
            return  false;
        }
        return true;
    }

    /**
     * 更改指定节点数据内容
     * @param path
     * @param data
     * @return
     */
    public boolean writeData(String path, String data) {
        try {
            System.out.println(LOG_PREFIX_OF_MAIN + " 更新数据成功，Path: " + path + ", stat: " +
                    this.zk.setData(path, data.getBytes(), -1));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 删除指定节点
     * @param path
     */
    public void deleteNode(String path) {
        try {
            this.zk.delete(path, -1);
            System.out.println(LOG_PREFIX_OF_MAIN + " 删除节点成功，Path: " + path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断指定节点是否存在
     * @param path
     * @param needWatch
     * @return
     */
    public Stat exists(String path, boolean needWatch) {
        try {
            return this.zk.exists(path, needWatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 读取节点
     * @param path
     * @param needWatch
     * @return
     */
    public String readData(String path, boolean needWatch) {
        try {
            return new String(this.zk.getData(path, needWatch, null));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取子节点
     * @param path
     * @param needWatch
     * @return
     */
    private List<String> getChildren(String path, boolean needWatch) {
        try {
            System.out.println(LOG_PREFIX_OF_MAIN + " 读取子节点操作...");
            return this.zk.getChildren(path, needWatch);
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 删除所有节点
     * @param needWatch
     */
    public void deleteAllTestPath(boolean needWatch) {
        if (this.exists(CHILDREN_PATH, needWatch) != null) {
            this.deleteNode(CHILDREN_PATH);
        }
        if (this.exists(PARENT_PATH, needWatch) != null) {
            this.deleteNode(PARENT_PATH);
        }
    }

    /**
     * 收到来自 Server 的 Watcher 通知后的处理
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("进入 process ... event = " + watchedEvent);

        try {
            Thread.sleep(200);
        } catch (InterruptedException e){
            e.printStackTrace();
        }

        if (watchedEvent == null) {
            return;
        }

        // 连接状态
        Event.KeeperState keeperState = watchedEvent.getState();
        // 事件类型
        Event.EventType eventType = watchedEvent.getType();
        // 受影响的 path
        String path = watchedEvent.getPath();
        // 原子对象 seq 记录进入 process 的次数
        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";

        System.out.println(logPrefix + " 收到 Watcher 通知");
        System.out.println(logPrefix + " 连接状态:\t" + keeperState.toString());
        System.out.println(logPrefix + " 事件类型:\t" + eventType.toString());

        if (Event.KeeperState.SyncConnected == keeperState) {
            // 成功连接上 zk 服务器
            if (Event.EventType.None == eventType) {
                System.out.println(logPrefix + " 成功连接上 zk 服务器");
                connectedSemaphore.countDown();
            }
            // 创建节点
            else if (Event.EventType.NodeCreated == eventType) {
                System.out.println(logPrefix + " 节点创建");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 更新节点
            else if (Event.EventType.NodeDataChanged == eventType) {
                System.out.println(logPrefix + " 节点数据更新");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 更新子节点
            else if (Event.EventType.NodeChildrenChanged == eventType) {
                System.out.println(logPrefix + " 子节点变更");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 删除节点
            else if (Event.EventType.NodeDeleted == eventType) {
                System.out.println(logPrefix + " 节点 " + path + " 被删除");
            }
            else;
        }
        else if (Event.KeeperState.Disconnected == keeperState) {
            System.out.println(logPrefix + " 与 zk 服务器断开连接");
        }
        else if (Event.KeeperState.AuthFailed == keeperState) {
            System.out.println(logPrefix + " 权限检查失败");
        }
        else if (Event.KeeperState.Expired == keeperState) {
            System.out.println(logPrefix + " 会话失败");
        }
        else;

        System.out.println("---------------------------------------------");
    }


    public static void main(String[] args) throws Exception {

        // 建立 watcher 当前客户端可以视为一个 watcher 观察者角色
        Zookeeper_OriginalAPI_2_Watcher zkWatch = new Zookeeper_OriginalAPI_2_Watcher();
        // 创建连接
        zkWatch.createConnect(CONNECT_ADDR, SESSION_TIMEOUT);
        System.out.println(zkWatch.zk.toString());

        Thread.sleep(1000);

        // 清理节点
        zkWatch.deleteAllTestPath(false);

        // 1. 创建父结点 /p
        if (zkWatch.createPath(PARENT_PATH, System.currentTimeMillis() + "", true)) {
            Thread.sleep(1000);

            // 2. 读取节点 /p 和读取 /p 节点下的子节点的区别
            // 2.1 读取数据
            zkWatch.readData(PARENT_PATH, true);
            // 2.2 读取子节点(监控 childNoodChange 事件)
            zkWatch.getChildren(PARENT_PATH, true);
            // 2.3 更新数据
            zkWatch.writeData(PARENT_PATH, System.currentTimeMillis() + "");
            Thread.sleep(1000);
            // 2.4 创建子节点
            zkWatch.createPath(CHILDREN_PATH, System.currentTimeMillis() + "", true);

            // 3 建立子节点的触发
            zkWatch.createPath(CHILDREN_PATH + "/c1", System.currentTimeMillis() + "", true);
            zkWatch.createPath(CHILDREN_PATH + "/c1/cc1", System.currentTimeMillis() + "", true);
            zkWatch.deleteNode(CHILDREN_PATH + "/c1/cc1");
            zkWatch.deleteNode(CHILDREN_PATH + "/c1");

            // 4. 更新子节点数据的触发  在进行修改之前，我们需要 watch 一下这个节点
            Thread.sleep(1000);
            zkWatch.readData(CHILDREN_PATH, true);
            zkWatch.writeData(CHILDREN_PATH, System.currentTimeMillis() + "");

        }
        Thread.sleep(1000);
        // 清理节点
        zkWatch.deleteAllTestPath(false);

        Thread.sleep(1000);
        zkWatch.releaseConnection();

    }
}
