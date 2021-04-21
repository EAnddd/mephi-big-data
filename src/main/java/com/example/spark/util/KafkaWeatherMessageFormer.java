package com.example.spark.util;

import com.example.spark.services.MessageService;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class KafkaWeatherMessageFormer implements DataPreparer {

    private static final String PATTERN = "%s, area%d, sensor%d_%s, %d";
    private static Map<String, Integer> typesWithMaxValues = new HashMap<>();
    private static List<String> types = new ArrayList<>();
    private final MessageService producer;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private int lineQuantity = 1000;
    private final Executor fixedThreadPool = Executors.newFixedThreadPool(4);
    static {
        types.add("temp");
        types.add("hum");
        types.add("pres");
        typesWithMaxValues.put("temp", 50);
        typesWithMaxValues.put("hum", 100);
        typesWithMaxValues.put("pres", 800);
    }

    public KafkaWeatherMessageFormer(MessageService producer, int lineQuantity){
        this.producer = producer;
        this.lineQuantity = lineQuantity;
    }

    @Override
    public void prepareData() {
//        prepareAndSend();
        executorService.scheduleWithFixedDelay(this::send, 1, 3, TimeUnit.SECONDS);
    }

    private void send(){
        for (int i = 0; i < lineQuantity; i++) {
            CompletableFuture.runAsync(this::prepareAndSend, fixedThreadPool);
        }
    }
    public void prepareAndSend(){
//        for (int i = 0; i < lineQuantity; i++) {
            log.info("Send {}", createMessage());
            producer.sendData(createMessage());

//        }
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
