package com.example.spark.jobs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WeatherResult implements Serializable {
    UUID id;
    String key;
    Float value;
}
