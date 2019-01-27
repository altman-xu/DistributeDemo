package com.altman.distribute.rocketmq.demo_2;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author xuzhihua
 * @date 2019/1/20 4:57 PM
 *
 * 测试 消费者 负载均衡
 *
 * 先启动 Consumer1 Consumer2， 然后启动 Producer
 * 看 Consumer1， Consumer2 中的日志，看是不是两个客户端 近似均等地消费了 Producer 中生产的消息
 */
public class Producer {

    public static void main(String[] args) throws MQClientException , InterruptedException{

        DefaultMQProducer producer = new DefaultMQProducer(("rocketmq_demo_2_producer"));
        // 设置 NameServer 地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 重试机制 设置 消息发送失败重试次数
        producer.setRetryTimesWhenSendFailed(10);

        producer.start();

        for (int i = 0; i < 100; i++) {
            try {
                Message msg = new Message("Topic_demo_2",
                        "Tag1",
                        ("信息内容 " + i).getBytes()
                );

                // 发送消息，设置超时时间为 1000ms， 如果超时，则触发上面设置的重试机制，进行重发
                SendResult sendResult = producer.send(msg, 1000);
                System.out.println("Producer Console : " + sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();

    }

}
