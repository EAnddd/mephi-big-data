package com.example.spark.services;

import com.example.spark.jobs.WeatherResult;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

public class DataSaverLocalStub extends DataSaver {

    public List<WeatherResult> savedResults = new ArrayList<>();

    /**
     * This stub is used for testing purposes. Adds result into list to convenient checks.
     * @param javaPairRDD Input RDD that was constructed on previos steps.
     * @param path Directory name where to save result files.
     */
    @Override
    public void save(JavaRDD javaPairRDD, String path){
        savedResults = ((JavaRDD<WeatherResult>)javaPairRDD).collect();
//        ((JavaPairRDD<String, Float>)javaPairRDD).foreach(data -> savedResults.add(1, ""));
    }
}
