package com.altman.distribute.rocketmq.demo_2;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author xuzhihua
 * @date 2019/1/20 4:59 PM
 */

public class Consumer2 {


    public Consumer2() {
        try {

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rocketmq_demo_2_producer");

            consumer.setNamesrvAddr("127.0.0.1:9876");

            /**
             * 设置 Consumer1 第一次启动是从队列头部开始消费还是队列尾部开始消费
             * 如果非第一次启动，那么按照上次消费的位置继续消费
             */

            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.subscribe("Topic_demo_2", "Tag1 || Tag2 || Tag3");

            consumer.registerMessageListener(new Listener());
            consumer.start();

        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    class Listener implements MessageListenerConcurrently {
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
            try {
                for (MessageExt msg : list) {
                    String topic = msg.getTopic();
                    String msgBody = new String(msg.getBody(), "utf-8");
                    String tags = msg.getTags();
                    System.out.println("Consumer2 console : 收到消息: " + " topic : " + topic + " ,tags : " + tags + " ,msg : " + msgBody);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }


    public static void main(String[] args) {
        Consumer2 c1 = new Consumer2();
        System.out.println("c1 start...");
    }
}