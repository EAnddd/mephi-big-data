package com.example.mr;


import com.example.mr.data.SyslogCounter;
import com.example.mr.factory.MapReduceJobFactory;
import com.example.mr.mapper.LogTypeMapper;
import com.example.mr.reducer.LogTypeReducer;
import com.example.mr.util.SyslogDataUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

@Slf4j
public class LogsCounter {
    /**
     * @param args Two arguments, first - input file, second - output
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if(args.length < 2){
            throw new RuntimeException("No input and output directories, please provide two arguments!");
        }
        SyslogDataUtil.prerareData(args[0], args[1], Integer.parseInt(args[2]));

        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        // Create mapreduce job using fabric
        Job job = MapReduceJobFactory.createMapReduceJob(LogTypeMapper.class,
                LogTypeReducer.class, Text.class, IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        log.info("======= Started job =======");
        job.waitForCompletion(true);
        log.info("Was not able to parse {} messages", job.getCounters().findCounter(SyslogCounter.PARSE_ERROR).getValue());
        log.info("Was not able to map {} messages", job.getCounters().findCounter(SyslogCounter.MALFORMED).getValue());
    }
}
