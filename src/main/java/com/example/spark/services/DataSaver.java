package com.example.spark.services;

import org.apache.spark.api.java.JavaPairRDD;

public class DataSaver {
    /**
     * @param javaPairRDD Input RDD that was constructed on previos steps.
     * @param path Directory name where to save result files.
     */
    public void save(JavaPairRDD javaPairRDD, String path){
        javaPairRDD.saveAsTextFile(path);
    }
}
