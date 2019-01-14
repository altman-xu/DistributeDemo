package com.altman.distribute.zookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;

/**
 * @author xuzhihua
 * @date 2018/12/21 2:13 PM
 */
public class Zookeeper_Zkclient_Base {

    static final String CONNECT_ADDR = "127.0.0.1:2181";
    static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        ZkClient zkc = new ZkClient(new ZkConnection(CONNECT_ADDR), 10000);

        // 1. create  delete
        zkc.createEphemeral("/temp");
        zkc.createPersistent("/persistent/c1", true);
        Thread.sleep(10000);
        zkc.deleteRecursive("/persistent");

        // 2. set read
        zkc.createPersistent("/persistent", 1234);
        zkc.createPersistent("/persistent/c1", "c1 data");
        zkc.createPersistent("/persistent/c2", "c2 data");
        List<String> list = zkc.getChildren("/persistent");
        for (String p : list) {
            System.out.println(p);
            String rp = "/persistent/" + p;
            String data = zkc.readData(rp);
            System.out.println("节点为: " + rp + ", 内容为: " + data);
        }

        // 3 update exists
        zkc.writeData("/persistent/c1", "c1 modify data");
        System.out.println(zkc.readData("/persistent/c1").toString());
        System.out.println(zkc.exists("/persistent/c1"));

        //4 delete
        zkc.deleteRecursive("/persistent");



        zkc.close();
    }

}
