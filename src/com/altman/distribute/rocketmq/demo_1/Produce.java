package com.altman.distribute.rocketmq.demo_1;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author xuzhihua
 * @date 2019/1/20 10:49 AM
 */
public class Produce {

    public static void main(String[] args) throws MQClientException , InterruptedException{

        DefaultMQProducer producer = new DefaultMQProducer(("rocketmq_demo_1_producer"));

        producer.setNamesrvAddr("127.0.0.1:9876");

        producer.start();

        for (int i = 0; i < 100; i++) {
            try {
                Message msg = new Message("Topic_demo_1",
                        "TagA",
                        ("Hello RocketMQ " + i).getBytes()
                );
                SendResult sendResult = producer.send(msg);
                System.out.println("Producer Console : " + sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();

    }

}
