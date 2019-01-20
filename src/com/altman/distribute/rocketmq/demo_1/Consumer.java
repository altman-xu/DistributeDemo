package com.altman.distribute.rocketmq.demo_1;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author xuzhihua
 * @date 2019/1/20 10:50 AM
 */
public class Consumer {


    public static void main(String[] args) throws InterruptedException, MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocketmq_demo_1_producer");

        consumer.setNamesrvAddr("127.0.0.1:9876");

        /**
         * 设置 Consumer 第一次启动是从队列头部开始消费还是队列尾部开始消费
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("Topic_demo_1", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {


                try {
                    for (MessageExt msg : list) {
                        String topic = msg.getTopic();
                        String msgBody = null;

                            msgBody = new String(msg.getBody(), "utf-8");

                        String tags = msg.getTags();
                        System.out.println("Consumer console : 收到消息: " + " topic : " + topic + " ,tags : " + tags + " ,msg : " + msgBody);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    // 抛异常时候 重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                System.out.println("Consumer Console : " + Thread.currentThread().getName() + " Receive New Message: " + list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer started.");

    }
}
