package com.study.mqcode;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
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
     *
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

        //设置消息主题和消息标签
        String topic = "topic-one";
        String tag = "tag-one";

        //消息
        String msg = "测试消息";

        Message message = new Message(topic, tag, msg.getBytes());

        //异步发送
        producer.sendOneway(message);

    }

    //=======下边是发送顺序消息==========

    /**
     * 顺序消息，按照顺序发送消息，消费者也按照顺序消费消息
     *
     * @throws Exception
     */
    @Test
    public void testRocketMQProduceAPIOrder() throws Exception {
        //初始化消息
        String[] msgs = {
                "15103111039,创建",
                "15103111065,创建",
                "15103111039,付款",
                "15103117235,创建",
                "15103111065,付款",
                "15103117235,付款",
                "15103111065,完成",
                "15103111039,推送",
                "15103117235,完成",
                "15103111039,完成"
        };


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

        for (String s : msgs) {
            Message message = new Message(topic, tag, s.getBytes());

            String[] split = s.split(",");
            long orderId = Long.parseLong(split[0]);

            /*
            MessageQueueSelector用来选择发送的队列,
            这里用订单的id对队列数量取余来计算队列索引

            send(msg, queueSelector, obj)
            第三个参
             */

            SendResult send = producer.send(message, new MessageQueueSelector() {
                /**
                 *
                 * @param list 消息队列里的所有消息
                 * @param message  消息
                 * @param o 传入的orderId
                 * @return
                 */
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Long orderId = (Long) o;
                    //订单id对队列数量取余, 相同订单id得到相同的队列索引
                    long index = orderId % list.size();
                    System.out.println("消息已发送到: " + list.get((int) index));
                    return list.get((int) index);
                }
            }, orderId);

            System.out.println(send);
        }

    }

    @Test
    public void testRocketMQConsumerAPIOrder() throws Exception {
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
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                //获取当前线程名字
                String name = Thread.currentThread().getName();

                for (MessageExt msg : list) {
                    System.out.println(name + " - " + msg.getQueueId() + " - " + new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
    }

    //======下边是发送延时消息========

    /**
     * 延时消息  开源版本提供了18个延时级别
     *
     * @throws Exception
     */
    @Test
    public void testRocketMQProduceAPIDelay() throws Exception {
        //18个延时级别
        //this.messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

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

        String msg="延时消息";

        Message message=new Message(topic,tag,msg.getBytes());

        message.setDelayTimeLevel(3); //设置延时级别为3级  即10s

        SendResult send = producer.send(message);

        System.out.println(send);

    }

    @Test
    public void testRocketMQConsumerAPIDelay() throws Exception {
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
                System.out.println("------------------------------");
                for (MessageExt msg : list) {
                    long t = System.currentTimeMillis() - msg.getBornTimestamp();
                    System.out.println(new String(msg.getBody()) + " - 延迟: "+t);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }

    //========下边是发送批量消息

    /**
     * 批量消息
     * 不能是延时消息  并且所有消息必须属于同一个topic，大小限制不能超过4M
     * @throws Exception
     */
    @Test
    public void testRocketMQProduceAPIList() throws Exception {
        //初始化消息
        String[] msgs = {
                "15103111039,创建",
                "15103111065,创建",
                "15103111039,付款",
                "15103117235,创建",
                "15103111065,付款",
                "15103117235,付款",
                "15103111065,完成",
                "15103111039,推送",
                "15103117235,完成",
                "15103111039,完成"
        };
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

        List<Message> messages=new ArrayList<>();

        for (String s:msgs){

            Message message=new Message(topic,tag,s.getBytes());
            messages.add(message);
        }


        SendResult send = producer.send(messages);

        System.out.println(send);

    }

    //下边是消息过滤

    /**
     * 消息过滤主要体现在消费者方
     * @throws Exception
     */
    @Test
    public void testRocketMQConsumerAPISelector() throws Exception {
        /**
         * 创建消息消费者，有pull和push两种方式，pull需要消费者自己去拉取消息，push方式Broker会主动讲消息推送给消费者
         *
         * 设置消费者组，同一组消费者 必须消费相同的消息
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-test-one");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        //订阅消息 主题  标签
        consumer.subscribe("topic-one", MessageSelector.bySql("tag1=a || tag2=2")); //消息过滤  使用sql语法
        //监听消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("------------------------------");
                for (MessageExt msg : list) {
                    long t = System.currentTimeMillis() - msg.getBornTimestamp();
                    System.out.println(new String(msg.getBody()) + " - 延迟: "+t);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }
}
