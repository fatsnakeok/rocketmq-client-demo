package com.fatsnake.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @Auther: fatsnake
 * @Description": 发送单向消息：这种方式主要用在不特别关心发送结果的场景，例如日志发送。
 * @Date:2020-02-09 09:35
 * Copyright (c) 2020, zaodao All Rights Reserved.
 * 集群为双主双从
 *
 */
public class OneWayProducer {
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
            Message msg = new Message("baseTopic", "tag3", ("Hello World 单向消息" + i).getBytes());
            // 5.发送单向消息
            producer.sendOneway(msg);


            // 线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }
//        6.关闭生产者producer
        producer.shutdown();
    }
}
