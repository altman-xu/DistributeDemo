package com.altman.distribute.rocketmq.demo_1;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author xuzhihua
 * @date 2019/1/20 10:50 AM
 *
 * consumer 消息重试有两种
 *      timeout : 如果是超时情况，mq会无限制重试，可能会给到集群中的其他 consumer
 *      exception : return ConsumeConcurrentlyStatus.RECONSUME_LATER
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

                MessageExt message = list.get(0);
                try {
                    String topic = message.getTopic();
                    String msgBody = null;
                        msgBody = new String(message.getBody(), "utf-8");

                    String tags = message.getTags();
                    System.out.println("Consumer console : 收到消息: " + " topic : " + topic + " ,tags : " + tags + " ,msg : " + msgBody);


                    // 测试 consumer exception，触发消息重发 begin

                        if ("Hello RocketMQ 4".equals(msgBody)){

                            System.out.println("==============失败消息开始================");
                            System.out.println(msgBody + message);
                            System.out.println("==============失败消息结束================");

                            // 异常代码，模拟 consumer 业务处理抛出异常
                            int a = 1/0;

                        }
                    // 测试 consumer exception，触发消息重发 end


                } catch (Exception  e) {
                    e.printStackTrace();
                    // 抛异常时候 重试
                    // 会在时间间隔分别为 1s 2s 5s ... 2h 时进行消息重试
                    if (message.getReconsumeTimes() == 2){
                        // 如果重试两次后，还是失败，进行记录数据库日志操作
                        System.out.println("==== 重试两次2后，还是没成功，记录数据库日志，返回消息消费成功 ===");
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                System.out.println("Consumer Console : " + Thread.currentThread().getName() + " Receive New Message: " + message);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer started.");

    }
}
