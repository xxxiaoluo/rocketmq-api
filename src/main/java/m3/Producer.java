package m3;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务消息(单机模式)
 *  消息发布过程： 生产者发送半消息到队列，再执行生产者本地事务，执行成功提交事务，队列向消费者推消息或者消费者主动拉消息
 *      --生产者本地事务执行失败时，会回滚(撤销)半消息
 *      --半消息发送成功，本地事务执行成功，事务提交失败，rocketmq会携带半消息回查，一次查不到会每隔一分钟查一次
 */
public class Producer {
    public static void main(String[] args) throws MQClientException {

        //新建事务消息生产者
        TransactionMQProducer p = new TransactionMQProducer("producer3");
        //设置name server
        p.setNamesrvAddr("192.168.64.100:9876");
        //设置事务消息的监听器
        p.setTransactionListener(new TransactionListener() {

            ConcurrentHashMap<String, LocalTransactionState> localTx = new ConcurrentHashMap<>();

            //执行本地事务(业务)
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                System.out.println("执行本地事务,参数:" + arg);

                 /*
                本地事务执行的状态需要存储(防止还未向消费者发送消息MQ宕机，重启MQ后不知道消费者本地事务是否成功就要回查，要把状态给回查操作当根据)，可以存redis、磁盘
                 */
                localTx.put(msg.getTransactionId(),LocalTransactionState.COMMIT_MESSAGE);

                return LocalTransactionState.COMMIT_MESSAGE;//告诉服务器，可以投递消息
//                return LocalTransactionState.ROLLBACK_MESSAGE;//撤回消息，一般写try  catch中
            }

            /*
            回查方法
            检测频率默认1分钟，可通过在broker.conf文件中设置transactionCheckInterval的值来改变默认值，单位为毫秒。
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("服务器正在回查消息");

                LocalTransactionState s = localTx.get(msg.getTransactionId());
                if(s == null || s == LocalTransactionState.UNKNOW){
                    s = LocalTransactionState.ROLLBACK_MESSAGE;
                }
                return s;
            }
        });
        //启动
        p.start();
        //发送事务消息(半消息)，触发监听器执行本地事务(业务)
        while(true){
            System.out.println("输入消息:");
            String s = new Scanner(System.in).nextLine();
            Message msg = new Message("Topic3", s.getBytes());
            p.sendMessageInTransaction(msg, "触发执行生产者本地事务的业务参数数据");
        }
    }
}
