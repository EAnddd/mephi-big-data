package com.example.spark.services;

import java.util.List;

public interface MessageService {
    void connect();
    List readData();
    void sendData(Object data);
    void close();
}
