package com.fatsnake.mq.rocketmq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Auther: fatsnake
 * @Description": 顺序消息--消费者
 * @Date:2020-02-15 16:08
 * Copyright (c) 2020, zaodao All Rights Reserved.
 */
public class Consumer {
    public static void main(String[] args) throws Exception {
        //1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr("rockermq-nameserver1:19876;rockermq-nameserver2:29876");
        //3.订阅主题Topic和Tag
        consumer.subscribe("OrderTopic", "*");

        //4.注册消息监听器  MessageListenerOrderly 顺序监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("线程名称：【" + Thread.currentThread().getName() + "】:" + new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        //5.启动消费者
        consumer.start();
        System.out.println("消费者启动");
    }
}
