package com.example.spark.jobs;


import com.example.spark.services.DataSaver;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;


/**
 * Spark streaming aggregation class
 */
public class StreamingWeatherDataAggregator implements Aggregator {
    private final DataSaver saver = new DataSaver();
    private final JavaStreamingContext sc;

    private static final String TOPIC_NAME = "mybesttopic";

    /**
     * Constructor without parameters initializes or gets existing Spark Context
     */
    public StreamingWeatherDataAggregator(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local");
        sc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    }

    /**
     * Implementation of Aggregator interface aggregate method.
     * This method uses Spark Streaming framework to infinitely read data from source.
     * Source is Apache Kafka - here kafka properties are locally initializes to get initial connection.
     * For saving data to filesystem DataSaver util class is used.
     */
    @SneakyThrows
    @Override
    public void aggregate() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "0.0.0.0:9092");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put("group.id", "mybestconsumer");
        props.put("auto.offset.reset", "latest");
        props.put("enable.auto.commit", true);
        System.out.println("--> Processing stream");

        Set<String> topicsSet = new HashSet<>();
        topicsSet.add(TOPIC_NAME);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                sc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, props));
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);

        lines.filter(record -> record.matches(".*, area\\d, sensor\\d{3}_.*, \\d{0,3}"))
                .mapToPair(record -> {
                    String[] recorsArr = record.split(" ");
                    return Tuple2.apply(recorsArr[0].substring(0, 14) + "00:00.000 " + recorsArr[1] + " " + recorsArr[2].split("_")[1], recorsArr[3]);
                })
                .mapValues(value -> new Tuple2<>(Float.parseFloat(value),1))
                .reduceByKey((tuple1,tuple2) ->  new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
                .foreachRDD(a -> saver.save(a, "here"));

        sc.start();
        sc.awaitTermination();
    }

}