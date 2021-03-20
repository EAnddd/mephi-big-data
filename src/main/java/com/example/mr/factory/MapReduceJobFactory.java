package com.example.mr.factory;

import com.example.mr.LogsCounter;
import com.example.mr.data.SyslogDateWritable;
import com.example.mr.mapper.LogTypeMapper;
import com.example.mr.reducer.LogTypeReducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Class to create map-reduce Job from base data
 */
@Slf4j
public class MapReduceJobFactory {


    /**
     * @param mapper Mapper class
     * @param reducer Reducer class
     * @param outputKey Output key class
     * @param outputValue Output value class
     * @return job configuration for created job
     */
    public static Job createMapReduceJob(Class<? extends Mapper> mapper,
                                             Class<? extends Reducer> reducer,
                                             Class<?>  outputKey,
                                             Class<?> outputValue) {
        Job job;

        try {
            job = Job.getInstance();
            // configure separator for output file
            job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        } catch(IOException e) {
            log.error("Can't create job for mapper {}, reducer {}, outputKey {} and value {}", mapper, reducer, outputKey, outputValue);
            throw new RuntimeException("I can't create job so let's crash");
        }
        job.setJobName("Best ever job");
        job.setMapperClass(LogTypeMapper.class);
        job.setReducerClass(LogTypeReducer.class);
        // set combiner to pre-aggregate results on mapper side
        job.setCombinerClass(LogTypeReducer.class);
        job.setJarByClass(LogsCounter.class);
        job.setOutputKeyClass(SyslogDateWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job;
    }
}
