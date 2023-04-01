package com.kx.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: KX
 * @Description:普通消费者
 * @DateTime: 2023/4/1
 * @Version 1.0
 */

public class NormalCusmer {
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

        //绑定队列 将队列(queuq-k)与交换器通过 路由键 绑定
        String routKey = "kuang";
        //队列名称 交换器名称 路由键
        channel.queueBind(queueName, DirectProducer.EXCHANGE_NAME, routKey);
        System.out.println("waiting for message ......");

        //申明一个消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body, "UTF-8");
                System.out.println("received" + "[" + envelope.getRoutingKey() + "]" + msg);
            }
        };

        //在指定的队列中消费queue-k
        channel.basicConsume(queueName, true, consumer);
    }
}
