package com.study.mqcode;

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
        rocketMQTemplate.send("ss", MessageBuilder.withPayload("一条具体的消息").build());
    }
}
