package com.altman.distribute.zookeeper;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;

/**
 * @author xuzhihua
 * @date 2018/12/21 2:45 PM
 */
public class Zookeeper_Zkclient_Watcher_2 {

    static final String CONNECT_ADDR = "127.0.0.1:2181";
    static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        ZkClient zkc = new ZkClient(new ZkConnection(CONNECT_ADDR), 10000);


        zkc.subscribeDataChanges("/super", new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println("变更节点为: " + s + ", 内容为: " + o.toString());
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println("删除节点为: " + s);
            }
        });

        zkc.createPersistent("/super", "123456");

        Thread.sleep(1000);

        zkc.writeData("/super", "456", -1);

        Thread.sleep(1000);

        zkc.delete("/super");

        Thread.sleep(1000);

    }
}
