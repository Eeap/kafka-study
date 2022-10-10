package com.example.kafkastudy.springKafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@SpringBootApplication
public class SpringConsumerApplicationSingle {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplicationSingle.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplicationSingle.class);
        application.run(args);

    }

    //kafkalister를 통해 record 한개를 파라미터로 입력을 받음
    @KafkaListener(topics = "test", groupId = "test-group-00")
    public void readListener(ConsumerRecord<String, String> record) {
        logger.info(record.toString()); //안에는 record를 처리하는 로직을 넣을 수 있음.
    }

    //message를 파라미터로 받는 리스너로 역직렬화를 String으로 했기 때문에 String으로 받음
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void singleTopicListener(String msg){
        logger.info(msg);
    }
    //개별 리스너마다 옵션값을 부여하고 싶으면 listener어노테이션에 properties의 값을 넣어주면 된다
    @KafkaListener(topics = "test",groupId = "test-group-02",properties = {
            "max.poll.interval.ms:60000", "auto.offset,reset:earliest"
    })
    public void singleTopicPropertiesListener(String msg){
        logger.info(msg);
    }
    //컨슈머 쓰레드를 여러개 생성해서 병렬처리하고 싶을때 concurrency옵션을 주면 되고 파티션 개수만큼 최대로 설정할 수 있다.
    @KafkaListener(topics = "test",groupId = "test-group-03",concurrency = "3")
    public void concurrentTopicListener(String msg){
        logger.info(msg);
    }
    //특정 토픽의 파티션만 구독하고 싶다면 topicpartitions옵션을 이용하면되고 해당 파티션에 오프셋을 설정하면 오프셋의 데이터부터 가져온다
    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "test1",partitions = {"0","1"}),
            @TopicPartition(topic = "test2",partitionOffsets = @PartitionOffset(partition = "0",initialOffset = "3"))
    }, groupId = "test-group-04")
    public void listenSpecificPartition(ConsumerRecord<String,String> record) {
        logger.info(record.toString());
    }
}
