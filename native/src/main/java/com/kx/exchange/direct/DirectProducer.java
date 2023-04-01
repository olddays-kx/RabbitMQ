package com.kx.exchange.direct;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @Author: KX
 * @Description: direct交换器生产者
 * @DateTime: 2023/4/1
 * @Version 1.0
 */
public class DirectProducer {

    public final static String EXCHANGE_NAME = "direct_logs";
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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        //申明路由键和消息体
        String[] routKeys = {"kuang", "olddays", "onePice"};
        for (int i = 0; i < 3; i++) {
            String routKey = routKeys[i%3];
            String msg = "hello rabbitMQ" + (i+1);
            //发布消息
            channel.basicPublish(EXCHANGE_NAME, routKey, null, msg.getBytes(StandardCharsets.UTF_8));
            System.out.println("send" + routKey + msg + "success");
        }
        channel.close();
        connection.close();
    }
}
