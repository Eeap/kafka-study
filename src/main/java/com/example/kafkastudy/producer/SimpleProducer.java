package com.example.kafkastudy.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        //카프카 클러스터의 서버 host와 ip 설정
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //메시지 키 , 메시지 값 직렬화하기 위한 클래스 설정
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        //프로듀서 인스턴스 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String message = "TestFirst";
        //생성자에 들어가는 변수는 두가지 말고 더 존재. 토픽 이름, 메시지키, 메시지 값 여기선 메시지 키를 설정하지 않았으므로 default null
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,message);
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,"Key_test",message);
        int partition = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,partition,"Key_test",message+partition);
        //send한다고 즉각적으로 보내진 않음. 프로듀서 내부에 있다가 배치 형태로 전송된다.
        producer.send(record,new ProducerCallback());
//        RecordMetadata mt = null;
//        try {
//            mt = producer.send(record).get();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } catch (ExecutionException e) {
//            throw new RuntimeException(e);
//        }
//        logger.info("{}", record);
//        logger.info("{}",mt.toString());
        //프로듀서 내부 버퍼에 있는 레코드 배치를 브로커로 전송
        producer.flush();
        producer.close();
    }
}
