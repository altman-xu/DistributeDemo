package com.altman.distribute.redis.cluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * @author xuzhihua
 * @date 2019/1/3 4:24 PM
 */
public class Test_1_ClusterRedis {

    public static void main(String[] args) throws Exception  {

        Set<HostAndPort> jedisClusterNode = new HashSet<>();
        jedisClusterNode.add(new HostAndPort("127.0.0.1", 7001));
        jedisClusterNode.add(new HostAndPort("127.0.0.1", 7002));
        jedisClusterNode.add(new HostAndPort("127.0.0.1", 7003));
        jedisClusterNode.add(new HostAndPort("127.0.0.1", 7004));
        jedisClusterNode.add(new HostAndPort("127.0.0.1", 7005));
        jedisClusterNode.add(new HostAndPort("127.0.0.1", 7006));

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(100);
        cfg.setMaxIdle(20);
        cfg.setMaxWaitMillis(-1);
        cfg.setTestOnBorrow(true);
        JedisCluster jc = new JedisCluster(jedisClusterNode,6000,100,cfg);

        System.out.println(jc.set("cluster_age", "20"));
        System.out.println(jc.set("cluster_sex", "nv"));
        System.out.println(jc.get("cluster_age"));
        System.out.println(jc.get("cluster_sex"));

        jc.close();

    }

}
