package com.altman.distribute.zookeeper;

/**
 * mac 搭建 zookeeper 说明
 * @author xuzhihua
 * @date 2018/12/17 10:02 PM
 */
public class AltmanMain {

    public static void main(String[] args) {

        System.out.println("hello");
        // 通过 brew 安装 zookeeper
        // brew search zookeeper
        // brew install zookeeper
        // 查看 zookeeper 信息
        // brew info zookeeper
        // 启动 zookeeper 服务
        // 1. brew services start zookeeper    启动 zookeeper 服务，并设置开机自启
        // 2. zkServer start       启动 zookeeper 服务，不设置开机自启
        // 查看 zookeeper 状态
        // zkServer status
        // 通过 jps 查看 zookeeper 进程 终端输入 jps， 显示的 QuorumPeerMain 即是 zookeeper进程

        // 通过终端当做客户端连接 zookeeper ，终端下输入 zkcli 回车
        // 查看根节点，终端下输入 ls /

        // 通过jar客户端连接 zookeeper
        //// https://issues.apache.org/jira/secure/attachment/12436620/ZooInspector.zip
        //// 解压，进入目录ZooInspector\build，
        //// java -jar zookeeper-dev-ZooInspector.jar  //执行成功后，会弹出java ui client
    }

}
