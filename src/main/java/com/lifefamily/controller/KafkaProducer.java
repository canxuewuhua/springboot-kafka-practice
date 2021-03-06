package com.lifefamily.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@RestController
@Slf4j
public class KafkaProducer {

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("thread-%d").build();
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(20,20,0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(1024), threadFactory);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    // 发送消息
    @GetMapping("/kafka/normal/{message}")
    public void sendMessage1(@PathVariable("message") String normalMessage) {
        kafkaTemplate.send("kafka20210725test", normalMessage);
    }

    /**
     * 带回调的生产者
     * kafkaTemplate提供了一个回调方法addCallback，我们可以在回调方法中监控消息是否发送成功 或 失败时做补偿处理，有以下两种写法
     */
    @GetMapping("/kafka/callbackOne/{message}")
    public void sendMessage2(@PathVariable("message") String callbackMessage) {
        kafkaTemplate.send("kafka20210725test", callbackMessage).addCallback(success -> {
            // 消息发送到的topic
            String topic = success.getRecordMetadata().topic();
            // 消息发送到的分区
            int partition = success.getRecordMetadata().partition();
            // 消息在分区内的offset
            long offset = success.getRecordMetadata().offset();
            System.out.println("发送消息成功:" + topic + "-" + partition + "-" + offset);
        }, failure -> {
            System.out.println("发送消息失败:" + failure.getMessage());
        });
    }

    @GetMapping("/kafka/callbackTwo/{message}")
    public void sendMessage3(@PathVariable("message") String callbackMessage) {
        kafkaTemplate.send("topic1", callbackMessage).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("发送消息失败："+ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("发送消息成功：" + result.getRecordMetadata().topic() + "-"
                        + result.getRecordMetadata().partition() + "-" + result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * kafka事务提交
     * 如果在发送消息时需要创建事务，可以使用 KafkaTemplate 的 executeInTransaction 方法来声明事务
     */
    @GetMapping("/kafka/transaction")
    public void sendMessage7() {
        // 声明事务：后面报错消息不会发出去
        kafkaTemplate.executeInTransaction(operations -> {
            operations.send("topic1", "test executeInTransaction");
            throw new RuntimeException("fail");
        });
        // 不声明事务：后面报错但前面消息已经发送成功了
        kafkaTemplate.send("topic1", "test executeInTransaction");
        throw new RuntimeException("fail");
    }

    @GetMapping("/kafka/sendMsg")
    public void sendMsg(){
        List<String> userIds = new ArrayList<>();
        for (int i=0;i<500;i++){
            String userId = UUID.randomUUID().toString();
            log.info("Kafka的sendMsg生成的第：{}个userId{}", i, userId);
            userIds.add(userId+"-"+i);
        }
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                for (String  mess : userIds) {
                    exec(mess);
                }
            }
        });
    }

    private void exec(String mess) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        kafkaTemplate.send("kafka20210729test", mess).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("发送消息失败："+ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {
                System.out.println("发送消息成功：" + result.getRecordMetadata().topic() + "-"
                        + result.getRecordMetadata().partition() + "-" + result.getRecordMetadata().offset());
            }
        });
    }

}
