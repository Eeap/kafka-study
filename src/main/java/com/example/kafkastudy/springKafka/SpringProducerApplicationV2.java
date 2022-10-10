package com.example.kafkastudy.springKafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@RequiredArgsConstructor
@SpringBootApplication
public class SpringProducerApplicationV2 implements CommandLineRunner {
    private static String TOPIC_NAME = "test";
    private KafkaTemplate<String,String> customKakfaTemplate;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplicationV2.class);
        application.run();
    }

    @Override
    public void run(String... args) throws Exception {
        ListenableFuture<SendResult<String, String>> future = customKakfaTemplate.send(TOPIC_NAME,"test");
        future.addCallback(new KafkaSendCallback<String, String>(){
            //producer가 보낸 데이터의 브로커 적재 여부를 확인할 수 있는 callback함수
            @Override
            public void onSuccess(SendResult<String, String> result) {

            }

            @Override
            public void onFailure(KafkaProducerException ex) {

            }
        });
        System.exit(0);

    }
}
