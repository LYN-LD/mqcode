package com.study.mqcode;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.support.MessageBuilder;

import javax.annotation.Resource;

/**
 *
 * springboot 操作RocketMQ
 * @author LengYouNuan
 * @create 2021-05-10 下午2:24
 */
@SpringBootTest
public class SpringBootTestMQ {
    /**
     * 注入操作模版
     */
    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Test
    public void testBootMQOne(){
        //发送同步消息
        rocketMQTemplate.send("ss", MessageBuilder.withPayload("一条具体的消息").build());
        rocketMQTemplate.send(MessageBuilder.withPayload("一条具体的消息").build());

        //发送异步消息
        rocketMQTemplate.asyncSend("topicone","一条可爱的消息",new SendCallback(){

            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("成功了");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("失败了");
            }
        });

        //发送一个顺序消息
        rocketMQTemplate.asyncSendOrderly("topicone", "一条正经的消息", "00", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("成功了");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("失败了");
            }
        });
    }
}
