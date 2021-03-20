package com.example.mr.reducer;

import com.example.mr.data.SyslogCounter;
import com.example.mr.data.SyslogDateWritable;
import com.example.mr.mapper.LogTypeMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogTypeReducerTest {

    private static ReduceDriver<SyslogDateWritable, IntWritable, SyslogDateWritable, IntWritable> reduceDriver;
    private static MapReduceDriver<LongWritable, Text, SyslogDateWritable, IntWritable, SyslogDateWritable, IntWritable> mapReduceDriver;
    private final String normalString = "<15>1 2021-03-18T21:07:11.998Z That's test message";
    private final String badString = "bad bad string";

    @BeforeEach
    public void setup(){
        LogTypeMapper mapper = new LogTypeMapper();
        LogTypeReducer reducer = new LogTypeReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void reducerConvertationTest() {
        List<IntWritable> listValues = new ArrayList<>();
        listValues.add(new IntWritable(2));
        listValues.add(new IntWritable(1));

        reduceDriver.withInput(new SyslogDateWritable("2021-03-18T21 1"), listValues)
                .withOutput(new SyslogDateWritable("2021-03-18T21 Alert"), new IntWritable(2));
    }

    @Test
    public void worksFineWithNormalMessageTwice() throws IOException {
            mapReduceDriver.withInput(new LongWritable(), new Text(normalString))
                    .withInput(new LongWritable(), new Text(normalString))
                    .withOutput(new SyslogDateWritable("2021-03-18T21 Debug"), new IntWritable(2))
                    .runTest();
    }

    @Test
    public void worksHalfFineWithNormalMessageOnce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(normalString))
                .withInput(new LongWritable(), new Text(badString))
                .withOutput(new SyslogDateWritable("2021-03-18T21 Debug"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void worksBadWithBadMessages() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(badString))
                .withInput(new LongWritable(), new Text(badString))
                .runTest();
        assertEquals(2, mapReduceDriver.getCounters().findCounter(SyslogCounter.PARSE_ERROR).getValue());
    }
}
