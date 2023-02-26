package m1;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 同步消息（单机非集群）
 *      --消费者
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        /*
        rocketmq消费者有两种模式：push和pull，push是由服务器主动向消费者发送信息；pull模式由消费者主动向服务器请求消息
        这里就用push简单
         */
        //创建消费者实例
        DefaultMQPushConsumer c = new DefaultMQPushConsumer("consumer1");
        //设置name server
        c.setNamesrvAddr("192.168.64.100:9876");

        //订阅消息
        /*
        Topic1下面的标签消息都接收，参数2就设置为 * 号
         */
        c.subscribe("Topic1", "TagA");
        //消息监听器
        //MessageListenerConcurrently 监听器会启动多个线程，可以并行处理多条消息
        c.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : list){
                    String s = new String(msg.getBody());
                    System.out.println("收到:" + s);
                }
                /*
                一般循环放在try里面，返回失败放在catch里面。服务器得知消息处理失败会重复发送消息，最多18次，每次间隔会越来越长
                18次处理完还是失败，消息最后会放入死性队列。
                消息处理成功后在管理页还可以看到，这种消息会在3天后被删除
                 */
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //监听消息返回成功
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER; //监听消息返回失败
            }
        });
        //启动
        c.start();
    }
}