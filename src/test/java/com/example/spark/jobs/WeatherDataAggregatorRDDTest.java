package com.example.spark.jobs;

import com.example.spark.services.DataSaverLocalStub;
import com.example.spark.services.KafkaService;
import com.example.spark.services.MessageService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

public class WeatherDataAggregatorRDDTest {

    MessageService kafkaService = Mockito.mock(KafkaService.class);
    DataSaverLocalStub dataSaver = new DataSaverLocalStub();

    WeatherDataAggregatorRDD aggregatorRDD = new WeatherDataAggregatorRDD(kafkaService, dataSaver);
    List<String> successResult = new ArrayList<>();
    List<String> strangeResult = new ArrayList<>();

    @Test
    public void shouldAggregateByRDD() {
        successResult.add("2021-04-15T19:07:39.321, area4, sensor462_pres, 782");
        successResult.add("2021-04-15T19:07:39.321, area4, sensor462_pres, 780");
        successResult.add("2021-04-15T19:07:39.321, area4, sensor462_pres, 781");
        when(kafkaService.readData()).thenReturn(successResult);

        aggregatorRDD.aggregate("here");
        Assertions.assertEquals(1, dataSaver.savedResults.size());
        Assertions.assertEquals("2021-04-15T19:00:00.000 area4 pres", dataSaver.savedResults.get(0).getKey());
    }

    @Test
    public void shouldAggregateByRDDWithSeveralMessages() {
        successResult.add("2021-04-15T19:07:39.321, area4, sensor462_pres, 782");
        successResult.add("2021-04-15T19:07:39.321, area4, sensor462_pres, 780");
        successResult.add("2021-04-15T19:07:39.321, area4, sensor462_pres, 781");
        successResult.add("2021-04-19T19:07:39.321, area4, sensor462_pres, 781");
        when(kafkaService.readData()).thenReturn(successResult);

        aggregatorRDD.aggregate("here");
        Assertions.assertEquals(2, dataSaver.savedResults.size());
    }

    @Test
    public void shouldNotAggregateBecauseOfWrongData() {
        strangeResult.add("wrong data");
        when(kafkaService.readData()).thenReturn(strangeResult);
        aggregatorRDD.aggregate("here");
        Assertions.assertEquals(0, dataSaver.savedResults.size());
    }

}
