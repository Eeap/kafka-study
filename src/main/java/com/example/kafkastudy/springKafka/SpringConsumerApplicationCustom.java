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
public class SpringConsumerApplicationCustom {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplicationCustom.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplicationCustom.class);
        application.run(args);

    }
    //containerFactory 옵션에 빈 객체로 등록한 컨테이너팩토리를 넣음
    @KafkaListener(topics = "test", groupId = "test-group-01",
        containerFactory = "customContainerFactory")
    public void customListener(String msg) {
        logger.info(msg);
    }
}

