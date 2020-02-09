package com.fatsnake.mq.rocketmq.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @Auther: fatsnake
 * @Description": 消息的接受者
 * @Date:2020-02-09 17:08
 * Copyright (c) 2020, zaodao All Rights Reserved.
 * 负载均衡模式（默认模式）:消费者采用负载均衡方式消费消息，多个消费者共同消费队列消息，每个消费者处理的消息不同
 * 广播模式（需要手动设置）：消费者采用广播的方式消费消息，每个消费者消费的消息都是相同的
 * 测试时：启动两个消费者
 */
public class Consumer {
    public static void main(String[] args) throws Exception {
//        1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
//        2.指定Nameserver地址
        consumer.setNamesrvAddr("rockermq-nameserver1:19876;rockermq-nameserver2:29876");
//        3.订阅主题Topic和Tag
        consumer.subscribe("baseTopic", "tag1");

        // 监听多个tag
//        consumer.subscribe("baseTopic", "tag1 || tag2 || tag3");
        // 监听所有tag
//        consumer.subscribe("baseTopic", "*");

        // 不设置默认是 负载均衡模式消费
//        consumer.setMessageModel(MessageModel.CLUSTERING);

        // 广播模式消费
        consumer.setMessageModel(MessageModel.BROADCASTING);


//        4.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接受消息内容
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : msgList) {
                    // 将body中的消息byte[]转为字符串
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
//        5.启动消费者consumer  启动监听
        consumer.start();
    }
}
