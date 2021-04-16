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
        kafkaWeatherMessageFormer = new KafkaWeatherMessageFormer(kafkaService);
    }

    @Test
    public void shouldSendOneMessage() {
        kafkaService.sendData("Test data my data");
        kafkaService.getProducer().flush();
        Assertions.assertEquals("Test data my data", kafkaService.readData().get(0));
    }

    @Test
    public void shouldSendMultipleMessages() {
        kafkaWeatherMessageFormer.prepareData(10);
        kafkaService.getProducer().flush();
        Assertions.assertEquals(10, kafkaService.readData().size());
    }

    @AfterAll
    public static void stop() {
        kafka.stop();
    }
}
