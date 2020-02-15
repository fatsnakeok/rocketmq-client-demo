package com.fatsnake.mq.rocketmq.base.delay;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @Auther: fatsnake
 * @Description": 延时消息--生产者
 * @Date:2020-02-15 16:49
 * Copyright (c) 2020, zaodao All Rights Reserved.
 */
public class Producer {

    public static void main(String[] args) throws Exception {
        //1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //2.指定Nameserver地址
        producer.setNamesrvAddr("rockermq-nameserver1:19876;rockermq-nameserver2:29876");
        //3.启动producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            //4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数一：消息主题Topic
             * 参数二：消息Tag
             * 参数三：消息内容
             */
            Message msg = new Message("DelayTopic", "Tag1", ("Hello World" + i).getBytes());
            //设定延迟时间  org/apache/rocketmq/store/config/MessageStoreConfig.java
            msg.setDelayTimeLevel(4);
            //5.发送消息
            SendResult result = producer.send(msg);
            //发送状态
            SendStatus status = result.getSendStatus();

            System.out.println("发送结果:" + result);

            //线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }
        //6.关闭生产者
        producer.shutdown();

    }


}
