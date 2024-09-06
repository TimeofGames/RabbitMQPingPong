package org.example;

import com.rabbitmq.client.*;
import java.util.Random;
import java.util.random.RandomGenerator;

public class Receiver {

    private final static String QUEUE_NAME = "ping_pong_queue";
    private final static String PONG_QUEUE_NAME = "pong_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueDeclare(PONG_QUEUE_NAME, false, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");

                if ("Ping".equals(message)) {
                    int delay = (int) (Math.random()*1000);
                    String response = "Pong " + delay;
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    channel.basicPublish("", PONG_QUEUE_NAME, null, response.getBytes());
                    System.out.println(" [x] Sent '" + response + "'");
                }
            };
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});

            synchronized (Receiver.class) {
                Receiver.class.wait();
            }
        }
    }
}
