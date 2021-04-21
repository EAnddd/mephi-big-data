package com.example.spark.services;

import com.example.spark.jobs.WeatherResult;
import org.apache.spark.api.java.JavaRDD;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class DataSaver {
    /**
     * @param javaRDD Input RDD that was constructed on previos steps.
     * @param path Directory name where to save result files.
     */
    public void save(JavaRDD javaRDD, String path){
        javaFunctions(javaRDD)
                .writerBuilder("my_best_keyspace", "weather_results", mapToRow(WeatherResult.class))
                .saveToCassandra();
    }
}
