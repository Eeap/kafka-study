package com.example.kafkastudy.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerRebalanceListenerTest {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String GROUP_ID = "testGroup";

    public static void main(String[] args) {
        Properties configs = new Properties();
        //카프카 클러스터의 서버 host와 ip 설정
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //메시지 키 , 메시지 값 직렬화하기 위한 클래스 설정
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG , false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        //Collection 타입의 String 값들을 받음.
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener(){
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}",record);
                currOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset()+1,null)
                );
                consumer.commitSync(currOffset);

            }

        }
    }
    private static class RebalanceListener implements ConsumerRebalanceListener{
        Properties configs = new Properties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        Map<TopicPartition, OffsetAndMetadata> currOffset = new HashMap<>();
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("partitions are revoked.");
            consumer.commitSync(currOffset);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("partitions are assigned");
        }
    }
}
