package com.example.kafkastudy.springKafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

@SpringBootApplication
public class SpringConsumerApplicationBatchCommit {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplicationCustom.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplicationCustom.class);
        application.run(args);

    }
    //ackmode를 manual_immediate로 한 경우 수동 커밋을 위해 파라미터로 acknowledgemnet를 받아야함
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void commitListener(ConsumerRecords<String, String> records, Acknowledgment ack) {
        records.forEach(record -> {
            logger.info(record.toString());
        });
        ack.acknowledge(); //커밋 수행하기 위해서 acknowledge를 호출해야함
    }
    //consumer의 동기 커밋과 비동기 커밋을 이용하는 방식으로 파라미터로 컨슈머 인스턴스를 받아야한다
    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void consumerCommitListener(ConsumerRecords<String, String> records, Consumer<String,String> consumer) {
        records.forEach(record -> {
            logger.info(record.toString());
        });
        consumer.commitAsync();//사용자가 원하는 타이밍에 메소드 호출을 위해 ackmode가 manual_immediate로 설정되어 있어야함
    }
}
