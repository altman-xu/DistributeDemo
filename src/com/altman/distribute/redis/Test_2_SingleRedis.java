package com.altman.distribute.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.*;

import java.util.*;

/**
 * @author xuzhihua
 * @date 2018/12/25 11:24 AM
 */
public class Test_2_SingleRedis {
    // 1. 单机连接1台 redis 服务器
    private static Jedis jedis;
    // 2. 主从、哨兵 用 shard
    private static ShardedJedis shard;
    // 3. 连接池
    private static ShardedJedisPool pool;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // 单个节点
        jedis = new Jedis("127.0.0.1", 6379);

        // 分片
        List<JedisShardInfo> shards = Arrays.asList(new JedisShardInfo("127.0.0.1", 6379));
        shard = new ShardedJedis(shards);

        GenericObjectPoolConfig goConfig = new GenericObjectPoolConfig();
        goConfig.setMaxTotal(20);
        goConfig.setMaxIdle(100);
        goConfig.setMaxWaitMillis(-1);
        goConfig.setTestOnBorrow(true);
        pool = new ShardedJedisPool(goConfig, shards);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        jedis.disconnect();
        shard.disconnect();
        pool.destroy();
    }

    @Test
    public void testString() {

        // 添加数据
        String key1 = "test_2_name";
        String key2 = "test_2_age";
        String key3 = "test_2_qq";
        jedis.set(key1, "xuzhihua");
        System.out.println(jedis.get(key1));

        // 附加数据
        jedis.append(key1, " ... append data");
        System.out.println(jedis.get(key1));

        // 删除数据
        jedis.del(key1);
        System.out.println(jedis.get(key1));

        // 设置多个键值对
        jedis.mset(key1, "xuzhihua", key2, "25", key3, "110");
        jedis.incr(key2);
        System.out.println(jedis.get(key1) + "-" + jedis.get(key2) + "-" + jedis.get(key3));


        // 最终删除测试数据
        jedis.del(key1);
        jedis.del(key2);
        jedis.del(key3);
    }

    @Test
    public void testMap() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "xuzhihua");
        map.put("age", "18");
        map.put("qq", "1212");
        jedis.hmset("userMap", map);
        // 取出 user 中的name
        List<String> rsmap = jedis.hmget("userMap", "name", "age", "qq");
        System.out.println(rsmap);

        // 删除 map 中的某个键值
        jedis.hdel("userMap", "age");
        System.out.println(jedis.hget("userMap", "age"));
        System.out.println(jedis.hlen("userMap"));
        System.out.println(jedis.exists("userMap"));
        System.out.println(jedis.hkeys("userMap"));
        System.out.println(jedis.hvals("userMap"));

        Iterator<String> iterator = jedis.hkeys("userMap").iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.println(key + ":  " + jedis.hmget("userMap", key));
        }

        // 最终删除测试数据
        jedis.del("userMap");
    }

    @Test
    public void testList() {
        // 开始前，先移除错有的内容
        String keyList = "java framework";
        jedis.del(keyList);
        System.out.println(jedis.lrange(keyList, 0, -1));
        // 先向 keyList 中放入三条数据
        jedis.lpush(keyList, "spring");
        jedis.lpush(keyList, "struts");
        jedis.lpush(keyList, "hibernate");
        // 再读取所有数据 jedis.lrange 是按范围取出
        System.out.println(jedis.lrange(keyList, 0, -1));

        // 最终删除测试数据
        jedis.del(keyList);
    }

    @Test
    public void testSet() {
        String keySet = "user";
        // 添加
        jedis.sadd(keySet, "xuzhihua");
        jedis.sadd(keySet, "xuxiaohua");
        jedis.sadd(keySet, "altman");
        jedis.sadd(keySet, "altmanxu");
        jedis.sadd(keySet, "altman_xu");
        jedis.sadd(keySet, "who");
        // 移除
        jedis.srem(keySet, "who");

        // 获取所有加入的 value
        System.out.println(jedis.smembers(keySet));
        // 判断 who 是否是 user 的元素
        System.out.println(jedis.sismember(keySet, "who"));
        System.out.println(jedis.srandmember(keySet));
        // 返回集合的元素个数
        System.out.println(jedis.scard(keySet));

        // 最终删除测试数据
        jedis.del(keySet);
    }

    @Test
    public void testRLpush() throws InterruptedException {
        // jedis 排序
        // 注意，此处的 rpush 和 lpush 是 list 的操作，是一个双向链表(单从表现来看的)
        String key = "a";
        jedis.del(key);
        jedis.rpush(key, "1");
        jedis.rpush(key, "2");
        jedis.lpush(key, "6");
        jedis.lpush(key, "3");
        jedis.lpush(key, "9");
        System.out.println(jedis.lrange(key, 0, -1));
        // 输入排序后结构
        System.out.println(jedis.sort(key));
        System.out.println(jedis.lrange(key, 0, -1));
        System.out.println(jedis.sort(key));
        System.out.println(jedis.lrange(key, 0, -1));

        // 最终删除测试数据
        jedis.del(key);
    }

    @Test
    public void testTrans() {
        long start = System.currentTimeMillis();
        Transaction tx = jedis.multi();
        for (int i = 0; i < 10; i++) {
            tx.set("transaction_" +i , "" + i);
        }
        List<Object> results = tx.exec();
        long end = System.currentTimeMillis();
        System.out.println("Transaction set: " + ((end - start)/1000.0) + " seconds");

        // 最终删除测试数据
        for (int i = 0; i < 10; i++) {
            jedis.del("transaction_"+i);
        }
    }

    @Test
    public void testPipelineTrans() {
        long start = System.currentTimeMillis();
        // 事务
        Pipeline pipeline = jedis.pipelined();
        pipeline.multi();
        for (int i = 0; i < 10; i++) {
            pipeline.set("transaction_" + i, "" + i);
        }
        pipeline.exec();
        List<Object> result = pipeline.syncAndReturnAll();
        long end = System.currentTimeMillis();
        System.out.println("Pipelined transaction set: " + ((end - start)/1000.0) + " seconds");


        // 最终删除测试数据
        for (int i = 0; i < 10; i++) {
            jedis.del("transaction_"+i);
        }
    }

    @Test
    public void testShard() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            String result = shard.set("shard_" + i, "n " + i);
        }
        long end = System.currentTimeMillis();
        System.out.println("shard set: " + ((end - start)/1000.0) + " seconds");


        // 最终删除测试数据
        for (int i = 0; i < 10; i++) {
            jedis.del("shard_"+i);
        }
    }

    @Test
    public void testShardpipelined() {
        ShardedJedisPipeline pipeline = shard.pipelined();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            pipeline.set("shardedJedisPipeline_" + i, "" + i);
        }
        List<Object> results = pipeline.syncAndReturnAll();
        long end = System.currentTimeMillis();
        System.out.println("shardPipelined set: " + ((end - start)/1000.0) + " seconds");


        // 最终删除测试数据
        for (int i = 0; i < 10; i++) {
            jedis.del("shardedJedisPipeline_"+i);
        }
    }

    @Test
    public void testShardPool() {
        ShardedJedis shardedJedis = pool.getResource();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            String result = shardedJedis.set("ShardedJedis_" + i, "" + i);
        }
        long end = System.currentTimeMillis();
        pool.returnResource(shardedJedis);
        System.out.println("shardPool set:" + ((end - start)/1000.0) + " seconds");


        // 最终删除测试数据
        for (int i = 0; i < 10; i++) {
            jedis.del("ShardedJedis_"+i);
        }
    }



}
