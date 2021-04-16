package com.example.spark.util;

import com.example.spark.services.MessageService;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;

@Slf4j
public class KafkaWeatherMessageFormer implements DataPreparer {

    private static final String PATTERN = "%s, area%d, sensor%d_%s, %d";
    private static Map<String, Integer> typesWithMaxValues = new HashMap<>();
    private static List<String> types = new ArrayList<>();
    private final MessageService producer;

    static {
        types.add("temp");
        types.add("hum");
        types.add("pres");
        typesWithMaxValues.put("temp", 50);
        typesWithMaxValues.put("hum", 100);
        typesWithMaxValues.put("pres", 800);
    }

    public KafkaWeatherMessageFormer(MessageService producer){
        this.producer = producer;
    }

    /**
     * @param lineQuantity The quantity of lines wanted to be generated and sent with message producer.
     */
    @Override
    public void prepareData(int lineQuantity) {
        for (int i = 0; i < lineQuantity; i++) {
            log.info("Send {}", createMessage());
            producer.sendData(createMessage());
        }
    }

    /**
     * @return Method to create weather data message.
     * Example: 2021-04-15T19:07:39.321, area4, sensor462_pres, 782
     */
    private String createMessage(){

        int randomSeconds;
        int randType;
        String randomTypeName;
        int randomTypeMaxValue;
        randType = (int) (Math.random() * 3);
        randomTypeName = types.get(randType);
        randomTypeMaxValue = typesWithMaxValues.get(randomTypeName);
        randomSeconds = new Random().nextInt(3600 * 24);

        return String.format(PATTERN,
                LocalDateTime.now().minusSeconds(randomSeconds),
                createRandomDigitInrange(1, 5),
                createRandomDigitInrange(1, 500),
                types.get(randType),
                createRandomDigitInrange(randomTypeMaxValue - 100, randomTypeMaxValue));
    }

    /**
     * Close Message producer
     */
    public void clean(){
        producer.close();
    }
    private int createRandomDigitInrange(int min, int max){
        return (int) ((Math.random() * (max - min)) + min);
    }
}
