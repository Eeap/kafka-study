package com.example.kafkastudy.springKafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

public class SpringConsumerApplicationBatch {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplicationBatch.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplicationBatch.class);
        application.run(args);

    }

    //batch리스너이다 보니 여러개의 record들을 파라미터로 받음
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void batchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> {
            logger.info(record.toString());
        });
    }

    //message값들을 list로 받아서 처리하는 리스너
    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void batchListener(List<String> list) {
        list.forEach(record -> {
            logger.info(record);
        });
    }
    //여러개의 컨슈머 쓰레드로 운영할 경우에 concurrency 옵션을 줘서 여러 개를 사용할 수 있음
    @KafkaListener(topics = "test", groupId = "test-group-03", concurrency = "3")
    public void concurrentBatchListener(ConsumerRecords<String, String> records){
        records.forEach(record -> {
            logger.info(record.toString());});
    }
}
