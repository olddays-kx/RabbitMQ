package com.kx.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: KX
 * @Description: 绑定多个路由键到一个队列（即多个路由键由一个消费者消费）
 * @DateTime: 2023/4/1
 * @Version 1.0
 */

public class MultiRoutToConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接工厂到rabbit
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //创建连接工厂下的连接地址（默认湍口5672）
        connectionFactory.setHost("192.168.67.128");
        //虚拟主机
        connectionFactory.setVirtualHost("kuang");
        //rabbitmq用户名
        connectionFactory.setUsername("kuang");
        //密码
        connectionFactory.setPassword("123456");
        //创建连接
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //在信道中声明交换器
        channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        //声明队列
        String queueName = "queue-k";
        channel.queueDeclare(queueName, false, false, false, null);

        //将多个路由键绑到一个队列中
        /*队列绑定到交换器上时，是允许绑定多个路由键的，也就是多重绑定*/
        String[] routKeys = {"kuang", "olddays", "onePice"};
        for (String rout : routKeys) {
            channel.queueBind(queueName, DirectProducer.EXCHANGE_NAME, rout);
        }
        System.out.println("waiting for message ........");

        //申明一个消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body, "UTF-8");
                System.out.println("received" + envelope.getRoutingKey() + msg + "success");
            }
        };
        //指定消费者在指定的队列消费
        channel.basicConsume(queueName, true, consumer);
    }
}
