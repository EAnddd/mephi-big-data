package com.example.spark.services;

import com.example.spark.util.KafkaWeatherMessageFormer;
import org.junit.ClassRule;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;


public class KafkaServiceTest {
    private static Network network = Network.newNetwork();
    private static KafkaService kafkaService;
    private static KafkaWeatherMessageFormer kafkaWeatherMessageFormer;
    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")).withNetwork(network);

    @BeforeAll
    public static void setUp() {
        kafka.start();
        kafkaService = new KafkaService(kafka.getBootstrapServers(), 1);
        kafkaService.connect();
        kafkaWeatherMessageFormer = new KafkaWeatherMessageFormer(kafkaService, 10);
    }

    @Test
    public void shouldSendMultipleMessages() throws InterruptedException {
        kafkaWeatherMessageFormer.prepareData();
        kafkaService.getProducer().flush();
        Thread.sleep(3000);
        kafkaService.getProducer().close();
        Assertions.assertEquals(10, kafkaService.readData().size());
    }

    @AfterAll
    public static void stop() {
        kafka.stop();
    }
}
