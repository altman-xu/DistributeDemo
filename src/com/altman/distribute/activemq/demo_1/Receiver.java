package com.altman.distribute.activemq.demo_1;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.xml.soap.Text;

/**
 * @author xuzhihua
 * @date 2019/1/14 3:57 PM
 */
public class Receiver {

    public static void main(String[] args) throws Exception {

        // 第一步：建立 ConnectionFactory 工厂对象，需要填入用户名、密码、连接的地址，均使用默认即可，默认端口为"tcp://localhost:61616"

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                ActiveMQConnectionFactory.DEFAULT_USER,
                ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                "tcp://127.0.0.1:61616"
        );

        // 第二步：通过 ConnectionFactory 工厂对象创建一个 Connection 连接，并调用 Connection 的 start 方法开启连接，Connection 默认是关闭的

        Connection connection = connectionFactory.createConnection();
        connection.start();

        // 第三步：通过 Connection 对象创建 Session 会话，用户接受消息，参数配置1为是否启用事务，参数配置2为签收模式，一般设置自动签收

//        Session session = connection.createSession(Boolean.FALSE, Session.AUTO_ACKNOWLEDGE);
        // 使用 client 端签收的方式进行， 不进行手动签收的话， activemq 服务器上的数据还是存在
        // 实际项目建议使用 客户端手动签收
        Session session = connection.createSession(Boolean.FALSE, Session.CLIENT_ACKNOWLEDGE);

        // 第四步：通过 Session 创建 Destination 对象，指的是一个客户端用来指定生产消息目标和消费消息来源的对象，在PTP模式中，Destination 被称作 Queue， 在 Pub/Sub 模式中，Destination 被称作 Topic。 在程序中可以使用多个 queue 和 topic

        Destination destination = session.createQueue("queue1");

        // 第五步：我们需要通过 Session 对象创建对象消息的发送和接受对象(生产者和消费者) MessageProducer/MessageConsumer

        MessageConsumer messageConsumer = session.createConsumer(destination);


        while (true) {
            TextMessage msg = (TextMessage)messageConsumer.receive();

            // 如果 第三步 中是客户端手动签收，需要下面代码实现签收；另起一个线程(tcp)，告知服务器数据已被消费
            // 实际项目建议使用 客户端手动签收
            msg.acknowledge();

            if (msg == null) break;
            System.out.println("Receiver console: 收到的内容:【" + msg.getText() + "】");

        }

        if (connection != null) {
            connection.close();
        }

    }
}
