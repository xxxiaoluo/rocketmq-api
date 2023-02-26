package m1;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Scanner;

/**
 * 同步消息（单机非集群）
 *      --生产者
 * 延时消息 于第54行调个方法
 */
public class Producer {
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //新建生产者实例
        DefaultMQProducer p = new DefaultMQProducer("producer1");//producer1生产者组名
        //设置name server地址
        p.setNamesrvAddr("192.168.64.100:9876");
        //启动生产者，连接服务器
        p.start();

        /*
        主题相当于是消息的分类, 一类消息使用一个主题
         */
        String topic = "Topic1";

        /*
        tag 相当于是消息的二级分类, 在一个主题下, 可以通过 tag 再对消息进行分类
         */
        String tag = "TagA";

        //消息数据封装到message对象
        //发送
        while(true){
            System.out.println("输入消息:");
            String s = new Scanner(System.in).nextLine();
            /*
            Topic  一级分类
            Tag    二级分类
             */
            Message message = new Message(topic,tag,s.getBytes());//一级分类，二级分类，消息内容

            /*
                设置消息的延迟时间,这里不支持任意的时间,只支持18个固定的延迟时长,
                分别用Leven 1到18 来表示:

                org/apache/rocketmq/store/config/MessageStoreConfig.java
                this.messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
                 */
//            message.setDelayTimeLevel(3);

            SendResult r = p.send(message);// 发送消息后会得到服务器反馈, 包含: smsgId, sendStatus, queue, queueOffset, offsetMsgId
            System.out.println(r);
        }
    }
}
