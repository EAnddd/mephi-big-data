package com.example.spark.jobs;

import com.example.spark.services.DataSaver;
import com.example.spark.services.MessageService;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/**
 * Spark RDD aggregation class
 */
public class WeatherDataAggregatorRDD implements Aggregator {

    private final MessageService consumer;
    private DataSaver saver = new DataSaver();
    private final SparkContext sc;

    /**
     * @param consumer Consumer to get data from.
     */
    public WeatherDataAggregatorRDD(MessageService consumer){
        this.consumer = consumer;
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("yarn");
        sc = SparkContext.getOrCreate(sparkConf);
    }

    /**
     * This constructor is written to customize result saver.
     * @param consumer Consumer to get data from.
     * @param saver DataSaver class.
     */
    public WeatherDataAggregatorRDD(MessageService consumer, DataSaver saver){
        this.consumer = consumer;
        this.saver = saver;
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local");
        sc = SparkContext.getOrCreate(sparkConf);
    }

    /**
     * Implementation of Aggregator interface aggregate method.
     * This method uses Spark RDD framework to read data from source once.
     * Source is Apache Kafka.
     * For saving data to filesystem DataSaver util class is used.
     */
    @Override
    public void aggregate() {
        JavaPairRDD result = JavaSparkContext.fromSparkContext(sc).parallelize(prepareData())
                .filter(record -> record.matches(".*, area\\d, sensor\\d{3}_.*, \\d{0,3}"))
                .mapToPair(record -> {
                    String[] recorsArr = record.split(", ");
                    return Tuple2.apply(recorsArr[0].substring(0, 14) + "00:00.000 " + recorsArr[1] + " " + recorsArr[2].split("_")[1], recorsArr[3]);
                })
                .mapValues(value -> new Tuple2<>(Float.parseFloat(value),1))
                .reduceByKey((tuple1,tuple2) ->  new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
                .mapValues(data -> data._1()/ data._2() );
        saver.save(result, "here");
//                .foreach(data -> System.out.println("Key="+data._1() + " Average=" + data._2()._1/data._2()._2));
    }

    /**
     * @return List of data read from MessageService implementation (for example, Apache Kafka).
     */
    private List<String> prepareData(){
        return consumer.readData();
    }
}
