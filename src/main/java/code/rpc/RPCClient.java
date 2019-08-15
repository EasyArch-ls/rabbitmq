package code.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/*
我们建立了一个连接和渠道。
        我们的调用方法生成实际的RPC请求。
        在这里，我们首先生成一个唯一的correlationId 数并保存它 - 我们的消费者回调将使用此值来匹配相应的响应。
        然后，我们为回复创建一个专用的独占队列并订阅它。
        接下来，我们发布请求消息，其中包含两个属性：  replyTo和correlationId。
        在这一点上，我们可以坐下来等待正确的响应到来。
        由于我们的消费者交付处理是在一个单独的线程中进行的，因此我们需要在响应到达之前暂停主线程。使用BlockingQueue是一种可能的解决方案。这里我们创建了ArrayBlockingQueue ，容量设置为1，因为我们只需要等待一个响应。
        消费者正在做一个非常简单的工作，对于每个消费的响应消息，它检查correlationId 是否是我们正在寻找的那个。如果是这样，它会将响应置于BlockingQueue。
        同时主线程正在等待响应从BlockingQueue获取它。
        最后，我们将响应返回给用户。
*/

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    private RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = fibonacciRpc.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String call(String message) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        String result = response.take();
        channel.basicCancel(ctag);
        return result;
    }

    public void close() throws IOException {
        connection.close();
    }
}
