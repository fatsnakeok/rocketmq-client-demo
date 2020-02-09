package com.fatsnake.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @Auther: fatsnake
 * @Description": 发送同步消息：这种可靠性同步地发送方式使用的比较广泛，比如：重要的消息通知，短信通知。
 * @Date:2020-02-09 09:35
 * Copyright (c) 2020, zaodao All Rights Reserved.
 * 集群为双主双从
 *
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
//        1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
//        2.指定Nameserver地址, 多地址逗号隔开
        producer.setNamesrvAddr("rockermq-nameserver1:19876;rockermq-nameserver2:29876");
//        3.启动producer
        producer.start();
//
        for (int i = 0; i < 10; i++) {
//            4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数一：消费主题topic
             * 参数二：消费tag
             * 参数三：消息内容
             */
            Message msg = new Message("baseTopic", "tag1", ("Hello World" + i).getBytes());
            // 5.发送消息
            SendResult result = producer.send(msg);
            // 发送状态
            SendStatus status = result.getSendStatus();
            // 消息ID
            String msgId = result.getMsgId();
            // 消息接受队列ID
            int queueId = result.getMessageQueue().getQueueId();
            System.out.println("发送状态："+result+",消息ID"+msgId+",队列"+queueId);

            // 线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }
//        6.关闭生产者producer
        producer.shutdown();
    }
}
