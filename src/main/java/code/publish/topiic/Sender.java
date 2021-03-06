package code.publish.topiic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class Sender {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            Scanner scanner=new Scanner(System.in);
            String messageType = "fs.type01";
            System.out.println("请输入要发送的消息：");
            String message = scanner.next();

            channel.basicPublish(EXCHANGE_NAME, messageType, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + messageType + "':'" + message + "'");
        }
    }
}
