package com.altman.distribute.zookeeper.cluster_curator;

/**
 * @author xuzhihua
 * @date 2018/12/22 5:52 PM
 */
public class Client1 {

    public static void main(String[] args) throws Exception {
        CuratorWatcher watcher = new CuratorWatcher();
        System.out.println("c1 start");
        Thread.sleep(100000000);
    }
}
