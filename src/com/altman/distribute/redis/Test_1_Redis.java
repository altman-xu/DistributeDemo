package com.altman.distribute.redis;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * @author xuzhihua
 * @date 2018/12/25 11:22 AM
 */
public class Test_1_Redis {
    @Test
    public void test() {
        Jedis j = new Jedis("127.0.0.1", 6379);
        System.out.println(j);



        // select * from user where age = 25 and sex = m

        // 多种结合配合使用 hash 和 set 类型同时使用

        // 实体表 USER
        final String SYS_USER_TABLE = "SYS_USER_TABLE";
        // 指定业务 查询业务: SYS_USER_SEL_AGE_25
        final String SYS_USER_SEL_AGE_25 = "SYS_USER_SEL_AGE_25";
        // 指定业务 查询业务: SYS_USER_SEL_SEX_m
        final String SYS_USER_SEL_SEX_m = "SYS_USER_SEL_SEX_m";
        // 指定业务 查询业务: SYS_USER_SEL_SEX_w
        final String SYS_USER_SEL_SEX_w = "SYS_USER_SEL_SEX_w";

//        Map<String, String> map = new HashMap<>();
//
//        String uuid1 = UUID.randomUUID().toString();
//        User u1 = new User(uuid1, "z3", 28, "m");
//        map.put(uuid1, JSONObject.toJSONString(u1));
//        j.sadd(SYS_USER_SEL_SEX_m, uuid1);
//
//        String uuid2 = UUID.randomUUID().toString();
//        User u2 = new User(uuid2, "z7", 25, "m");
//        map.put(uuid2, JSONObject.toJSONString(u2));
//        j.sadd(SYS_USER_SEL_AGE_25, uuid2);
//        j.sadd(SYS_USER_SEL_SEX_m, uuid2);
//
//        String uuid3 = UUID.randomUUID().toString();
//        User u3 = new User(uuid3, "w5", 25, "w");
//        map.put(uuid3, JSONObject.toJSONString(u3));
//        j.sadd(SYS_USER_SEL_AGE_25, uuid3);
//        j.sadd(SYS_USER_SEL_SEX_w, uuid3);
//
//        String uuid4 = UUID.randomUUID().toString();
//        User u4 = new User(uuid4, "z4", 29, "m");
//        map.put(uuid4, JSONObject.toJSONString(u4));
//        j.sadd(SYS_USER_SEL_SEX_m, uuid4);
//
//        String uuid5 = UUID.randomUUID().toString();
//        User u5 = new User(uuid5, "w6", 28, "w");
//        map.put(uuid5, JSONObject.toJSONString(u5 ));
//        j.sadd(SYS_USER_SEL_SEX_w, uuid5);
//
//        j.hmset(SYS_USER_TABLE, map);


        // 测试查询业务
        Set<String> user_ages = j.smembers(SYS_USER_SEL_AGE_25);

        List<String> retlist = new ArrayList<>();
        for (Iterator iterator = user_ages.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            String ret = j.hget(SYS_USER_TABLE, string);
            System.out.println(ret);
        }



        // select * from user where age = 25 and sex = m
        // 取 25岁 和 男 的交集
        Set<String> user_age25_sexm = j.sinter(SYS_USER_SEL_AGE_25, SYS_USER_SEL_SEX_m);
        for (Iterator iterator = user_age25_sexm.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            String ret = j.hget(SYS_USER_TABLE, string);
            System.out.println(ret);
            User u = JSONObject.parseObject(ret, User.class);
            System.out.println(u.getName());
            System.out.println(u.getSex());
            System.out.println(u.getAge());
        }


    }


}
