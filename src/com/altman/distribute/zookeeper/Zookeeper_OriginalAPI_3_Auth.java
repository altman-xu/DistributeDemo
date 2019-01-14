package com.altman.distribute.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * 节点授权
 * @author xuzhihua
 * @date 2018/12/20 11:06 AM
 */
public class Zookeeper_OriginalAPI_3_Auth implements Watcher {

    final static String CONNECT_ADDR = "127.0.0.1:2181";
    // 测试路径
    final static String PATH = "/testAuth8";
    final static String PATH_DEL = "/testAuth8/delNode8";
    // 认证类型 digest 类似用户名密码形式的权限标识
    final static String authentication_type = "digest";
    // 认证正常方法
    final static String correctAuthentication = "123456";
    // 认证错误方法
    final static String badAuthentication = "654321";

    static ZooKeeper zk = null;
    // 计时器
    AtomicInteger seq = new AtomicInteger();
    // 标识
    private static final String LOG_PREFIX_OF_MAIN = "【Main】";

    private static final String BAD_Auth_PRIFIX = "【使用错误的授权信息】:";
    private static final String NONE_Auth_PRIFIX = "【不使用任何授权信息】:";
    private static final String CORRECT_Auth_PRIFIX = "【使用正确的授权信息】:";

    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (watchedEvent == null) {
            return;
        }

        // 连接状态
        Event.KeeperState keeperState = watchedEvent.getState();
        // 事件类型
        Event.EventType eventType = watchedEvent.getType();
        // 受影响的path
        String path = watchedEvent.getPath();

        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";

        System.out.println(logPrefix + " 收到 Watcher 通知");
        System.out.println(logPrefix + " 连接类型:\t " + keeperState.toString());
        System.out.println(logPrefix + " 事件类型:\t " + eventType.toString());

        if (Event.KeeperState.SyncConnected == keeperState) {
            // 成功连接上 ZK 服务器
            if (Event.EventType.None == eventType) {
                System.out.println(logPrefix + " 成功连接上 ZK 服务器");
                connectedSemaphore.countDown();
            }
        } else if (keeperState.Disconnected == keeperState) {
            System.out.println(logPrefix + " 与 ZK 服务器断开连接");
        } else if (keeperState.AuthFailed == keeperState) {
            System.out.println(logPrefix + " 权限检查失败");
        } else if (keeperState.Expired == keeperState) {
            System.out.println(logPrefix + " 会话失效");
        }
        System.out.println("-----------------------------");
    }

    public void createConnection(String connectString, int sessionTimeout) {
        this.releaseConnection();
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, this);
            // 添加节点授权
            zk.addAuthInfo(authentication_type, correctAuthentication.getBytes());
            System.out.println(LOG_PREFIX_OF_MAIN + " 开始连接 ZK 服务器");
            // 等待
            connectedSemaphore.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Zookeeper_OriginalAPI_3_Auth testAuth = new Zookeeper_OriginalAPI_3_Auth();
        testAuth.createConnection(CONNECT_ADDR, 2000);
        List<ACL> acls = new ArrayList<>();
        for (ACL ids_acl : ZooDefs.Ids.CREATOR_ALL_ACL) {
            acls.add(ids_acl);
        }


        // 清除旧数据
        Stat stat = zk.exists(PATH_DEL, false);
        if (stat != null) {
//            deleteNodeByCorrentAuthentication();
        }
        Stat statParent = zk.exists(PATH, false);
        if (statParent != null) {
//            deleteParent();
        }
        // 创建测试节点
        try {
            zk.create(PATH, "init content".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key: " + correctAuthentication + " 创建节点: " + PATH + ", 初始内容是: init content");
        }catch (Exception e){
            e.printStackTrace();
        }
        try {
            zk.create(PATH_DEL, "will be deleted!".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key: " + correctAuthentication + " 创建节点: " + PATH_DEL + ", 初始内容是: will be deleted!");
        } catch (Exception e) {
            e.printStackTrace();
        }


        // 获取数据
//        getDataByNoAuthentication();
//        getDataByBadAuthentication();
//        getDataByCorrentAuthentication();

        // 更新数据
        updateDataByNoAuthentication();
        updateDataByBadAuthentication();
        updateDataByCorrentAuthentication();

//        // 删除数据
//        deleteNodeByNoAuthentication();
//        deleteNodeByBadAuthentication();
//        deleteNodeByCorrentAuthentication();
//
//        Thread.sleep(1000);
//
//        deleteParent();
        // 释放连接
        testAuth.releaseConnection();
    }


    // 获取数据 不采用密码
    static void getDataByNoAuthentication() {
        try {
            ZooKeeper nozk = new ZooKeeper(CONNECT_ADDR, 2000 ,null);
            Thread.sleep(1000);
            System.out.println(NONE_Auth_PRIFIX + " 开始获取数据: " + PATH);
            System.out.println(NONE_Auth_PRIFIX + " 成功获取数据: " + nozk.getData(PATH, false, null));
        } catch (Exception e) {
            System.out.println(NONE_Auth_PRIFIX + " 失败获取数据，原因: " + e.getMessage());
        }
    }

    // 获取数据 采用错误的密码
    static void getDataByBadAuthentication() {
        try {
            ZooKeeper badzk = new ZooKeeper(CONNECT_ADDR, 2000 ,null);
            // 授权
            badzk.addAuthInfo(authentication_type, badAuthentication.getBytes());
            Thread.sleep(1000);
            System.out.println(BAD_Auth_PRIFIX + " 开始获取数据: " + PATH);
            System.out.println(BAD_Auth_PRIFIX + " 成功获取数据: " + badzk.getData(PATH, false, null));
        } catch (Exception e) {
            System.out.println(BAD_Auth_PRIFIX + " 失败获取数据，原因: " + e.getMessage());

        }
    }

    // 获取数据 采用正确的密码
    static void getDataByCorrentAuthentication() {
        try {
            ZooKeeper correctzk = new ZooKeeper(CONNECT_ADDR, 2000 ,null);
            // 授权
            correctzk.addAuthInfo(authentication_type, correctAuthentication.getBytes());
            Thread.sleep(1000);
            System.out.println(CORRECT_Auth_PRIFIX + " 开始获取数据: " + PATH);
            System.out.println(CORRECT_Auth_PRIFIX + " 成功获取数据: " + new String(correctzk.getData(PATH, false, null)));
        } catch (Exception e) {
            System.out.println(CORRECT_Auth_PRIFIX + " 失败获取数据，原因: " + e.getMessage());
        }
    }


    // 更新数据 采用正确密码
    static void updateDataByCorrentAuthentication() {
        System.out.println(CORRECT_Auth_PRIFIX + " 开始更新数据:  " + PATH);
        try {
            ZooKeeper correctzk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            correctzk.addAuthInfo(authentication_type, correctAuthentication.getBytes());
            Thread.sleep(1000);
            Stat stat = correctzk.exists(PATH, false);
            if (stat != null) {
                correctzk.setData(PATH, CORRECT_Auth_PRIFIX.getBytes(), -1);
                System.out.println(CORRECT_Auth_PRIFIX + " 成功更新数据 : " + new String(correctzk.getData(PATH, false, null)));
            }
        } catch (Exception e) {
            System.err.println(CORRECT_Auth_PRIFIX + " 失败更新数据，原因: " + e.getMessage());
        }
    }

    // 更新数据 采用错误密码
    static void updateDataByBadAuthentication() {
        System.out.println(BAD_Auth_PRIFIX + " 开始更新数据:  " + PATH);
        try {
            ZooKeeper badzk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            badzk.addAuthInfo(authentication_type, correctAuthentication.getBytes());
            Thread.sleep(1000);
            Stat stat = badzk.exists(PATH, false);
            if (stat != null) {
                badzk.setData(PATH, BAD_Auth_PRIFIX.getBytes(), -1);
                System.out.println(BAD_Auth_PRIFIX + " 成功更新数据 : " + new String(badzk.getData(PATH, false, null)));
            }
        } catch (Exception e) {
            System.err.println(BAD_Auth_PRIFIX + " 失败更新数据，原因: " + e.getMessage());
        }
    }


    // 更新数据 不采用密码
    static void updateDataByNoAuthentication() {
        System.out.println(NONE_Auth_PRIFIX + " 开始更新数据:  " + PATH);
        try {
            ZooKeeper nonezk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            Thread.sleep(1000);
            Stat stat = nonezk.exists(PATH, false);
            if (stat != null) {
                nonezk.setData(PATH, NONE_Auth_PRIFIX.getBytes(), -1);
                System.out.println(NONE_Auth_PRIFIX + " 成功更新数据 : " + new String(nonezk.getData(PATH, false, null)));
            }
        } catch (Exception e) {
            System.err.println(NONE_Auth_PRIFIX + " 失败更新数据，原因: " + e.getMessage());
        }
    }

    // 删除节点 不采用密码
    static void deleteNodeByNoAuthentication() {
        System.out.println(NONE_Auth_PRIFIX + " 开始删除节点:  " + PATH_DEL);
        try {
            ZooKeeper nonezk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            Thread.sleep(1000);
            Stat stat = nonezk.exists(PATH_DEL, false);
            if (stat != null) {
                nonezk.delete(PATH_DEL, -1);
                System.out.println(NONE_Auth_PRIFIX + " 成功删除节点");
            }
        } catch (Exception e) {
            System.err.println(NONE_Auth_PRIFIX + " 失败删除节点，原因: " + e.getMessage());
        }
    }

    // 删除节点 采用错误密码
    static void deleteNodeByBadAuthentication() {
        System.out.println(BAD_Auth_PRIFIX + " 开始删除节点:  " + PATH_DEL);
        try {
            ZooKeeper badezk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            badezk.addAuthInfo(authentication_type, BAD_Auth_PRIFIX.getBytes());
            Thread.sleep(1000);
            Stat stat = badezk.exists(PATH_DEL, false);
            if (stat != null) {
                badezk.delete(PATH_DEL,  -1);
                System.out.println(BAD_Auth_PRIFIX + " 成功删除节点");
            }
        } catch (Exception e) {
            System.err.println(BAD_Auth_PRIFIX + " 失败删除节点，原因: " + e.getMessage());
        }
    }

    // 删除节点 采用正确密码
    static void deleteNodeByCorrentAuthentication() {
        System.out.println(CORRECT_Auth_PRIFIX + " 开始删除节点:  " + PATH_DEL);
        try {
            ZooKeeper correctzk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            correctzk.addAuthInfo(authentication_type, CORRECT_Auth_PRIFIX.getBytes());
            Thread.sleep(1000);
            Stat stat = correctzk.exists(PATH_DEL, false);
            if (stat != null) {
                correctzk.delete(PATH_DEL, -1);
                System.out.println(CORRECT_Auth_PRIFIX + " 成功删除节点");
            }
        } catch (Exception e) {
            System.err.println(CORRECT_Auth_PRIFIX + " 失败删除节点，原因: " + e.getMessage());
        }
    }

    // 删除父结点
    static void deleteParent() {
        try {
            ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, 2000, null);
            zk.addAuthInfo(authentication_type, correctAuthentication.getBytes());
            Thread.sleep(1000);
            Stat stat = zk.exists(PATH, false);
            if (stat != null) {
                zk.delete(PATH, -1);
            }
        } catch (Exception e) {
            System.err.println(CORRECT_Auth_PRIFIX + " 删除父结点失败，原因: " + e.getMessage());
        }
    }
}
