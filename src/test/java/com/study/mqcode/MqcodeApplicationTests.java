package com.study.mqcode;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class MqcodeApplicationTests {

    //==========下边是同步发送的方式============
    @Test
    public void testRocketMQProduceAPI() throws Exception {
        /**
         * 创建消息发送者 并指定生产者组
         */
        DefaultMQProducer producer = new DefaultMQProducer("producer-one-test");
        /**
         * 设置nameserver，讲自己交给其管理
         */
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        //设置消息主题和消息标签
        String topic = "topic-one";
        String tag = "tag-one";

        //消息
        String msg = "测试消息";

        Message message = new Message(topic, tag, msg.getBytes());

        //同步发送
        SendResult send = producer.send(message);
        System.out.println(send);
    }

    @Test
    public void testRocketMQConsumerAPI() throws Exception {
        /**
         * 创建消息消费者，有pull和push两种方式，pull需要消费者自己去拉取消息，push方式Broker会主动讲消息推送给消费者
         *
         * 设置消费者组，同一组消费者 必须消费相同的消息
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-test-one");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        //订阅消息 主题  标签
        consumer.subscribe("topic-one", "tag-one");
        //监听消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println(list); //打印订阅到的消息列表
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }

    //==========下边是异步发送的方式============

    @Test
    public void testRocketMQProduceAPIOne() throws Exception {
        /**
         * 创建消息发送者 并指定生产者组
         */
        DefaultMQProducer producer = new DefaultMQProducer("producer-one-test");
        /**
         * 设置nameserver，讲自己交给其管理
         */
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        producer.setRetryTimesWhenSendAsyncFailed(1000);//设置异步发送失败的超时重试时间

        //设置消息主题和消息标签
        String topic = "topic-one";
        String tag = "tag-one";

        //消息
        String msg = "测试消息";

        Message message = new Message(topic, tag, msg.getBytes());

        //异步发送
        producer.send(message, new SendCallback() {

            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("消息发送成功的回调函数");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("消息发送失败的回调函数");
            }
        });

    }

    //========下边是发送单向消息======

    /**
     * 单向消息  不关注发送结果，服务器不返回发送结果
     * @throws Exception
     */
    @Test
    public void testRocketMQProduceAPIOneWay() throws Exception {
        /**
         * 创建消息发送者 并指定生产者组
         */
        DefaultMQProducer producer = new DefaultMQProducer("producer-one-test");
        /**
         * 设置nameserver，讲自己交给其管理
         */
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        producer.setRetryTimesWhenSendAsyncFailed(1000);//设置异步发送失败的超时重试时间

        //设置消息主题和消息标签
        String topic = "topic-one";
        String tag = "tag-one";

        //消息
        String msg = "测试消息";

        Message message = new Message(topic, tag, msg.getBytes());

        //异步发送
        producer.sendOneway(message);

    }


}
