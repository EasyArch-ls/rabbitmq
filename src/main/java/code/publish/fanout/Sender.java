package code.publish.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Sender {
	public static void main(String[] args) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("127.0.0.1");
		try (Connection connection = factory.newConnection();
			 Channel channel = connection.createChannel()) {
		// 定义ExchangeName,第二个参数是Exchange的类型，fanout表示消息将会分列发送给多账户
		String exchangeName = "news";
		channel.exchangeDeclare(exchangeName, "fanout");//Exchange的类型有direct,topic,headers,fanout四种
		String msg = "Hello World!";
		// 发送消息，这里与前面的不同，这里第一个参数不再是字符串，而是ExchangeName ，第二个参数也不再是queueName，而是空字符串
		channel.basicPublish(exchangeName, "", null, msg.getBytes());
		System.out.println("send message[" + msg + "] to exchange "
				+ exchangeName + " success!");
	}
}}
