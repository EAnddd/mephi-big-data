package com.example.mr.reducer;


import com.example.mr.data.SyslogDateWritable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@Slf4j
public class LogTypeReducer extends Reducer<SyslogDateWritable, IntWritable, SyslogDateWritable, IntWritable> {

    /**
     * @param key Text from mapper in format yyyy-MM-dd hh severity
     * @param values Quantity of keys (can be after Combiner)
     * @param context Context of map-reduce job
     */
    @Override
    public void reduce(SyslogDateWritable key, Iterable<IntWritable> values, Context context) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        try {
            context.write(key, new IntWritable(sum));
        } catch (IOException | InterruptedException e) {
            log.error("Can't reduce for key {}", key);
        }

    }
}
