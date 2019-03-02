package com.altman.distribute.rocketmq.demo_3;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * 测试事务消费
 * RocketMQ消息发送时预发送，只有当 tranExecuter 回调函数中 确认的 COMMIT， 才真正发送， 此时这个消息在MQ上可以被消费者感知到并消费
 * 在回调函数COMMIT之前，消费者在MQ上是感知不到这个消息的
 * @author xuzhihua
 * @date 2019/2/13 10:45 PM
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /**
         * 一个应用创建一个Producer，有应用来维护此对象，可以设置为全局对象或单例
         * 注意：ProducerGroupName需要有应用来保证唯一，一类Producer集合的名称，这类Producer通常发送一类消息，且发送逻一致
         * ProducerGroup这个概念发送普通的消息是，作用不大， 但是发送分布式事务消息时，比较关键，
         * 因为服务器会检查这个Group下的任意一个Producer
         */
        String group_name = "transaction_producer";

        final TransactionMQProducer producer = new TransactionMQProducer(group_name);
        // nameserver服务
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 事务回查最小并发数
        producer.setCheckThreadPoolMaxSize(5);
        // 事务回查最并发数
        producer.setCheckThreadPoolMaxSize(20);
        // 队列数
        producer.setCheckRequestHoldMax(2000);
        /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        producer.start();
        // 服务器回调Producer，检查本地事务分支成功还是失败
        // 此处代码是 MQ 服务器中 对一直是 prepare 状态的消息，进行回调判断 该消息到底是 COMMIT 还是 ROLLBACK
        producer.setTransactionCheckListener(new TransactionCheckListener() {
            @Override
            public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
                System.out.println("stat -- " + new String(msg.getBody()));
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });


        /**
         * 下面这段代码表名一个Producer对象可以发送多个topic，多个tag消息
         * 注意：send方法是同步调用，只要不抛异常标识成功，但是发送成功也可能会有多种状态
         * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用来说如果对消息可靠性要求高，
         * 需要对这种情况处理。另外，消息可能会存在发送失败的情况，失败重试有应用来处理
         */
        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();

        for (int i = 1; i < 11; i++) {
            try {
                Message msg = new Message("TopicTransaction",
                        "Transaction" + i,
                        "key",
                        ("Hello RocketMQ" + i).getBytes());
                SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, "tq");
                System.out.println("Producer Console : " + sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
            TimeUnit.MILLISECONDS.sleep(1000);
        }

        /**
         * 应用退出时，要调用shutdown来倾力资源， 关闭网络连接，从MQ服务器上注销自己
         * 注意：我们建议应用在JBoss，Tomcat等容器的退出钩子里调用shutdown方法
         */
//        producer.shutdown();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                producer.shutdown();
            }
        }));
        System.exit(0);
    }

}
