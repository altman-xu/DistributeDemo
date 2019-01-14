package com.altman.distribute.zookeeper;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;

/**
 * @author xuzhihua
 * @date 2018/12/21 2:45 PM
 */
public class Zookeeper_Zkclient_Watcher_1 {

    static final String CONNECT_ADDR = "127.0.0.1:2181";
    static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        ZkClient zkc = new ZkClient(new ZkConnection(CONNECT_ADDR), 10000);
        // 对父结点添加监控子节点变化
        zkc.subscribeChildChanges("/super", new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("parentPath: " + parentPath);
                System.out.println("currentChilds: " + currentChilds);
            }
        });

        Thread.sleep(3000);

        if (zkc.exists("/super")){
            zkc.deleteRecursive("/super");
        }
        zkc.createPersistent("/super");
        Thread.sleep(1000);

        zkc.createPersistent("/super" + "/" + "c1", "c1 data");
        Thread.sleep(1000);

        zkc.createPersistent("/super" + "/" + "c2", "c2 data");
        Thread.sleep(1000);

        zkc.deleteRecursive("/super");

        zkc.close();

    }
}
