package com.altman.distribute.activemq.demo_2;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * @author xuzhihua
 * @date 2019/1/16 11:46 AM
 */
public class Publish {

    private ConnectionFactory factory;
    private Connection connection;
    private Session session;
    private MessageProducer producer;

    public Publish() {
        try {
            factory = new ActiveMQConnectionFactory(
                    ActiveMQConnectionFactory.DEFAULT_USER,
                    ActiveMQConnectionFactory.DEFAULT_PASSWORD,
                    "tcp://localhost:61616"
            );
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(null);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void sendMessage() throws Exception {
        Destination destination = session.createTopic("topic1");
        TextMessage textMessage = session.createTextMessage("topic 内容");
        producer.send(destination, textMessage);
    }

    public static void main(String[] args) throws Exception {
        Publish p = new Publish();
        p.sendMessage();
    }
}
