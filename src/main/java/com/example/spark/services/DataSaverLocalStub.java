package com.example.spark.services;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.List;

public class DataSaverLocalStub extends DataSaver {

    public List<String> savedResults = new ArrayList<>();

    /**
     * This stub is used for testing purposes. Adds result into list to convenient checks.
     * @param javaPairRDD Input RDD that was constructed on previos steps.
     * @param path Directory name where to save result files.
     */
    @Override
    public void save(JavaPairRDD javaPairRDD, String path){
        savedResults = ((JavaPairRDD<String, Float>)javaPairRDD).map(data -> data._1() + " " + data._2()).collect();
//        ((JavaPairRDD<String, Float>)javaPairRDD).foreach(data -> savedResults.add(1, ""));
    }
}
