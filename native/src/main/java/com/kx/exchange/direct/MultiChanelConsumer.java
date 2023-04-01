package com.kx.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: KX
 * @Description: 一个连接绑定多个信道 两个队列
 * @DateTime: 2023/4/1
 * @Version 1.0
 */

public class MultiChanelConsumer {

    private static class ConsumerWorker implements Runnable {
        final Connection connection;
        public ConsumerWorker(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            try {
                //创建信道
                Channel channel = connection.createChannel();
                //申明一个direct交换器
                channel.exchangeDeclare(DirectProducer.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

                //申明一个随机队列
                String queueName = channel.queueDeclare().getQueue();
                //将多个路由键绑到一个队列中
                /*队列绑定到交换器上时，是允许绑定多个路由键的，也就是多重绑定*/
                String[] routKeys = {"kuang", "olddays", "onePice"};
                for (String rout : routKeys) {
                    channel.queueBind(queueName, DirectProducer.EXCHANGE_NAME, rout);
                }
                System.out.println("waiting for message ........");
                //设置一个消费者的名称
                final String consumerName = Thread.currentThread().getName();
                //申明一个消费者
                final Consumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        String message = new String(body, "UTF-8");
                        System.out.println(consumerName + ":" + queueName +" Received "  + envelope.getRoutingKey() + ":'" + message + "'");
                    }
                };

                channel.basicConsume(queueName, true, consumer);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
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

        //启动两个线程
        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread(new ConsumerWorker(connection));
            thread.setName("olddays" + i);
            thread.start();
        }
    }
}
