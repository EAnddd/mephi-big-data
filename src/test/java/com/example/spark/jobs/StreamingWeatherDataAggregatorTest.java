package com.example.spark.jobs;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.example.spark.services.KafkaService;
import com.example.spark.util.KafkaWeatherMessageFormer;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamingWeatherDataAggregatorTest {

    @Rule
    static CassandraContainer cassandraContainer = new CassandraContainer<>(DockerImageName.parse("cassandra:latest"));
    private static Network network = Network.newNetwork();
    static StreamingWeatherDataAggregator streamingWeatherDataAggregator;
    private static KafkaService kafkaService;
    private static KafkaWeatherMessageFormer kafkaWeatherMessageFormer;
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")).withNetwork(network);
    @BeforeAll
    public static void setUp(){
        kafka.start();
        kafkaService = new KafkaService(kafka.getBootstrapServers(), 1);
        kafkaService.connect();
        kafkaWeatherMessageFormer = new KafkaWeatherMessageFormer(kafkaService, 10);
        cassandraContainer.start();
        streamingWeatherDataAggregator =
                new StreamingWeatherDataAggregator(cassandraContainer.getContainerIpAddress(), "cassandra", "cassandra", "local", kafka.getBootstrapServers(), cassandraContainer.getMappedPort(9042).toString());

    }

    @Test
    public void shouldReadDataAndAggregate() {
        kafkaWeatherMessageFormer.prepareData();
        kafkaService.getProducer().flush();
        Session session = cassandraContainer.getCluster().connect();
        session.execute("CREATE KEYSPACE IF NOT EXISTS my_best_keyspace WITH replication = \n" +
                "{'class':'SimpleStrategy','replication_factor':'1'};");
        session.execute("CREATE TABLE my_best_keyspace.weather_results(id uuid primary key, key text, value float);");
        System.out.println(session.getCluster().getMetadata().getKeyspaces());
        streamingWeatherDataAggregator.aggregate(null);
        ResultSet resultSet = session.execute("select * from my_best_keyspace.weather_results;");
        assertTrue(resultSet.all().size() > 1);

    }

}
