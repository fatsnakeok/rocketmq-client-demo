package com.fatsnake.mq.rocketmq.base.delay;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Auther: fatsnake
 * @Description": 延时消息--消费者
 * @Date:2020-02-15 16:54
 * Copyright (c) 2020, zaodao All Rights Reserved.
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        //1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //2.指定Nameserver地址
        consumer.setNamesrvAddr("rockermq-nameserver1:19876;rockermq-nameserver2:29876");
        //3.订阅主题Topic和Tag
        consumer.subscribe("DelayTopic", "*");

        //4.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            //接受消息内容
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    // 消息消费时间 - 消费存储（发送）时间
                    System.out.println("消息ID：【" + msg.getMsgId() + "】,延迟时间："
                        + (System.currentTimeMillis() - msg.getStoreTimestamp()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //5.启动消费者consumer
        consumer.start();

        System.out.println("消费者启动");

    }
}
