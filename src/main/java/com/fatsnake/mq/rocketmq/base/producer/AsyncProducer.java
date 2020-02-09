package com.fatsnake.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @Auther: fatsnake
 * @Description": 发送异步消息：异步消息通常用在对响应时间敏感的业务场景，即发送端不能容忍长时间地等待Broker的响应。
 * @Date:2020-02-09 16:41
 * Copyright (c) 2020, zaodao All Rights Reserved.
 */
public class AsyncProducer {

    public static void main(String[] args) throws Exception {
//        1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
//        2.指定Nameserver地址, 多地址逗号隔开
        producer.setNamesrvAddr("rockermq-nameserver1:19876;rockermq-nameserver2:29876");
//        3.启动producer
        producer.start();
//
        for (int i = 0; i < 10; i++) {
            final int index = i;
//            4.创建消息对象，指定主题Topic、Tag和消息体
            /**
             * 参数一：消费主题topic
             * 参数二：消费tag   区别于同步发送，修改tag
             * 参数三：消息内容
             */
            Message msg = new Message("baseTopic", "tag2", ("Hello World 异步消息" + i).getBytes());
            // 5.异步发送消息
            producer.send(msg, new SendCallback() {
                // 发送成功回调函数
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("异步发送消息成功：%-10d OK %s %n", index,
                        sendResult);
                }

                // 发送失败回掉函数
                @Override
                public void onException(Throwable e) {
                    System.out.printf("异步发送消息失败：%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });


            // 线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }
//        6.关闭生产者producer
        producer.shutdown();
    }
}
