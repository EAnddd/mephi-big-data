package com.example.mr.mapper;

import com.example.mr.data.SyslogCounter;
import com.example.mr.data.SyslogDateWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogTypeMapperTest {
    private static MapDriver<LongWritable, Text, SyslogDateWritable, IntWritable> mapDriver;

    private String normalString = "<15>1 2021-03-18T21:07:11.998Z That's test message";
    private String badString = "bad bad string";

    @BeforeAll
    public static void setUp() {
        LogTypeMapper logTypeMapper = new LogTypeMapper();
        mapDriver = MapDriver.newMapDriver(logTypeMapper);
    }

    @Test
    public void testMapperCustomType() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(normalString))
                .withOutput(new SyslogDateWritable("2021-03-18T21 Debug"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapperCustomTypeBad() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(badString))
                .runTest();
        assertEquals(1, mapDriver.getCounters().findCounter(SyslogCounter.PARSE_ERROR).getValue());
    }
}
