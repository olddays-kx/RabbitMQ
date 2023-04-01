package com.kx.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: KX
 * @Description: 一个人队列绑定多个消费者
 * @DateTime: 2023/4/1
 * @Version 1.0
 */

public class MultiConsumerToChanel {

    final static class ConsumerWorker implements Runnable {
        final Connection connection;
        final String queueName;
        public ConsumerWorker(Connection connection, String queueName) {
            this.connection = connection;
            this.queueName = queueName;
        }

        @Override
        public void run() {
            //创建一个信道
            try {
                //不同线程创建不同的信道
                Channel channel = connection.createChannel();
                //申明一个队列不同的线程创建队列时由于使用一个队列名称 则申明的是同一队列
                channel.queueDeclare(queueName, false, false , false,null);
                //消费者名字，打印输出用
                final String consumerName =  Thread.currentThread().getName();
                String[] routKeys = {"kuang", "olddays", "onePice"};
                for (String rout : routKeys) {
                    //将不同的路由键绑到一个队列中
                    channel.queueBind(queueName, DirectProducer.EXCHANGE_NAME, rout);
                }

                System.out.println(consumerName + "waiting for message........");
                //申明一个消费者
                final Consumer consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        String msg = new String(body, "UTF-8");
                        System.out.println(consumerName + "received " + msg + " success");
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
        //申明一个人队列的名称
        String queueName = "queue-kx";
        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread(new ConsumerWorker(connection, queueName));
            thread.start();
        }
    }
}
