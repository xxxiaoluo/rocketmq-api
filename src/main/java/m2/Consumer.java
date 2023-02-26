package m2;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 顺序消息(单机模式)
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer c = new DefaultMQPushConsumer("consumer2");
        c.setNamesrvAddr("192.168.64.100:9876");

        c.subscribe("Topic2", "*");

        //MessageListenerOrderly 此为消费者单线程处理消息的api
        c.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                String t = Thread.currentThread().getName();

                for (MessageExt msg : list) {
                    System.out.println(t+" - "+ msg.getQueueId() + " - " +new String(msg.getBody()));
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        c.start();
        System.out.println("开始消费数据");
    }
}
