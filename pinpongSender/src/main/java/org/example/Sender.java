package org.example;

import com.rabbitmq.client.*;

import java.util.concurrent.CountDownLatch;

public class Sender {

    private static final String QUEUE_NAME = "ping_pong_queue";
    private static final String PONG_QUEUE_NAME = "pong_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueDeclare(PONG_QUEUE_NAME, false, false, false, null);

            while (true) {
                String message = "Ping";
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");

                CountDownLatch latch = new CountDownLatch(1);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String pongMessage = new String(delivery.getBody(), "UTF-8");
                    if (pongMessage != null && pongMessage.contains("Pong")) {
                        System.out.println(" [x] Received '" + pongMessage + "'");
                        latch.countDown();  // Сбрасываем latch, когда получено "Pong"
                    }
                };

                String consumerTag = channel.basicConsume(PONG_QUEUE_NAME, true, deliverCallback, consumerTag1 -> {});
                latch.await();
                channel.basicCancel(consumerTag);
                Thread.sleep(1000);
            }
        }
    }
}
