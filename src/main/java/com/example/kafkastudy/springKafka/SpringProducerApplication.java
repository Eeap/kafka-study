package com.example.kafkastudy.springKafka;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;


//basic kafka template!!
@RequiredArgsConstructor
@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {
    private static String TOPIC_NAME = "test";
    private KafkaTemplate<Integer,String> template;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run();
    }
    @Override
    public void run(String... args) throws Exception {
        for (int i=0; i<10; i++){
            template.send(TOPIC_NAME, "test" + i);
        }
        System.exit(0);

    }
}
