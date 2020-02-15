package com.fatsnake.mq.rocketmq.base.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @Auther: fatsnake
 * @Description": 顺序消息--生产者
 * @Date:2020-02-15 15:26
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
        //构建消息集合
        List<OrderStep> orderStepList = OrderStep.buildOrders();

        // 发送消息
        for (int i = 0; i < orderStepList.size(); i++) {
            String body = orderStepList.get(i).toString();
            Message message = new Message("OrderTopic", "Order", "i" + i, body.getBytes());

            /**
             * 参数一：消息对象
             * 参数二：消息队列的选择器
             * 参数三：选择队列的业务表示（订单ID）
             */
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                /**
                 *
                 * @param mqs 队列集合
                 * @param msg 消息对象
                 * @param arg 业务标识参数
                 * @return MessageQueue
                 */
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    long orderId = (long) arg;
                    long index = orderId % mqs.size();
                    return mqs.get((int) index);
                }
            }, orderStepList.get(i).getOrderId());
        }
    }
}
