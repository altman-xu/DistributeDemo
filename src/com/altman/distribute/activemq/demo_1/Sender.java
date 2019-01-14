package com.altman.distribute.activemq.demo_1;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.*;

/**
 * @author xuzhihua
 * @date 2019/1/14 3:57 PM
 */
public class Sender {

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

        // 使用事物的方式进行消息发送
//        Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
        // 使用 client 端签收的方式进行
        // 实际项目建议使用 客户端手动签收
        Session session = connection.createSession(Boolean.TRUE, Session.CLIENT_ACKNOWLEDGE);

        // 第四步：通过 Session 创建 Destination 对象，指的是一个客户端用来指定生产消息目标和消费消息来源的对象，在PTP模式中，Destination 被称作 Queue， 在 Pub/Sub 模式中，Destination 被称作 Topic。 在程序中可以使用多个 queue 和 topic

        Destination destination = session.createQueue("queue1");

        // 第五步：我们需要通过 Session 对象创建对象消息的发送和接受对象(生产者和消费者) MessageProducer/MessageConsumer

        MessageProducer messageProducer = session.createProducer(destination);

        // 第六步：我们可以使用 MessageProducer 的 setDeliveryMode 方法为其设置持久化特性和非持久化特性

        messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        // 第七步：最后我们使用 JMS 规范的 TextMessage 形式创建数据(通过 Sessino 对象)，并用 MessageProducer 的 send 方法发送数据。 同理客户端使用 receive 方法接收数据。 最后关闭 Connection对象

        for (int i = 0; i < 5; i++) {
            TextMessage textMessage = session.createTextMessage();
            textMessage.setText("from Sender: 我是消息内容，id为 " + i);
            messageProducer.send(textMessage);
            System.out.println("Sender console: 【" + textMessage.getText() + "】");
        }

        // 第三步 中如果使用了事务方法，这里需要手动 commit， 才能送到 activemq 服务器上
        session.commit();

        if (connection != null) {
            connection.close();
        }

    }
}
