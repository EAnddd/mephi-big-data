package com.example.spark.services;

import lombok.Data;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service responsible for kafka initial connection, writing and reading data.
 */
@Data
public class KafkaService implements MessageService {
    private static final String TOPIC = "mybesttopic";
    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private String server = "kafka:9092";
    private Integer batchSize = 100;
    public KafkaService(){
    }

    public KafkaService(String server, Integer batchSize){
        this.server = server;
        this.batchSize = batchSize;
    }

    /**
     * Methos for initial connection and creating consumer and producer using connection properties.
     */
    public void connect() {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", server);
        config.put("client.id", "thebestid");
        config.put("group.id", "thebestgroupid");
        config.put("batch.size", batchSize);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        producer = new KafkaProducer<>(config);
        consumer = new KafkaConsumer<>(config);
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        topicPartitionList.add(new TopicPartition(TOPIC, 0));
        consumer.assign(topicPartitionList);
        consumer.seekToBeginning(topicPartitionList);
    }

    /**
     * @return Read data and synchronously commit.
     */
    public List<String> readData() {
        List<String> values = new ArrayList<>();
        consumer.poll(Duration.ofSeconds(10)).iterator().forEachRemaining(consumerRecord -> values.add(consumerRecord.value()));
        consumer.commitSync();
        return values;
    }

    /**
     * @param data Send data to kafka producer.
     */
    public void sendData(Object data) {
        producer.send(new ProducerRecord<>(TOPIC, "id", (String)data));
        producer.flush();
    }

    /**
     * Flush all cached data and close connection.
     */
    public void close(){
        producer.flush();
        producer.close();
    }
}
