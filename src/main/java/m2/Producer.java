package m2;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * 顺序消息(单机模式)
 */
public class Producer {
    //三组相同id
    static String[] msgs = {
            "15103111039,创建",
            "15103111065,创建",
            "15103111039,付款",
            "15103117235,创建",
            "15103111065,付款",
            "15103117235,付款",
            "15103111065,完成",
            "15103111039,推送",
            "15103117235,完成",
            "15103111039,完成"
    };
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //创建生产者实例
        DefaultMQProducer p = new DefaultMQProducer("producer2");
        //指定name server
        p.setNamesrvAddr("192.168.64.100:9876");
        //启动
        p.start();
        //发送消息，设置队列选择器
        for (String s : msgs) {
            Message msg = new Message("Topic2", s.getBytes());

            String[] a = s.split(",");
            long orderId = Long.parseLong(a[0]);

            /*
            MessageQueueSelector用来选择发送的队列，
            这里用订单的id对队列数量取余来计算队列索引

            send(msg, queueSelector, obj)
            第三个参数会传递到queueSelector, 作为它的第三个参数
             */

//            p.send(msg,队列选择器,选择依据);
            SendResult sendResult = p.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) { //参数:服务器端Topic2中的队列列表，消息，选择依据
                    Long orderId = (Long) arg;
                    //订单id对队列数量取余,相同订单id得到相同的队列索引
                    long index = orderId % mqs.size();
                    System.out.println("消息已发送到:" + mqs.get((int) index));

                    return mqs.get((int) index); ///返回消息要发送到的位置
                }
            }, orderId);

            System.out.println(sendResult);
        }
    }
}

