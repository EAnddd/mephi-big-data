package com.example.spark;

import com.example.spark.jobs.Aggregator;
import com.example.spark.jobs.StreamingWeatherDataAggregator;
import com.example.spark.jobs.WeatherDataAggregatorRDD;
import com.example.spark.services.KafkaService;
import com.example.spark.services.MessageService;
import com.example.spark.util.KafkaWeatherMessageFormer;
import com.example.spark.util.DataPreparer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WeatherAggregator {
    /**
     * @param args Leave empty if want to run spark rdd job. Add anything if want to run streaming job.
     */
    @SneakyThrows
    public static void main(String[] args) {
        String pathToSave = "here";
        int quantity = 1000;

        if(args.length == 2 ){
            pathToSave = args[0];
            quantity = Integer.parseInt(args[1]);
        }
        MessageService kafkaService = new KafkaService();
        kafkaService.connect();
        DataPreparer dataPreparer = new KafkaWeatherMessageFormer(kafkaService, quantity);
        dataPreparer.prepareData();

        Aggregator aggregator;
        if(args.length < 3) {
            log.info("______SIMPLE________");
            aggregator = new WeatherDataAggregatorRDD(kafkaService);
        } else {
            log.info("______STREAM________");
            aggregator = new StreamingWeatherDataAggregator();
        }
        aggregator.aggregate(pathToSave);
    }
}
