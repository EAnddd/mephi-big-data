package com.example.mr.mapper;


import com.example.mr.data.SyslogCounter;
import com.example.mr.data.SyslogDateWritable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper class for syslog RFC5424 logs
 */
@Slf4j
public class LogTypeMapper extends Mapper<LongWritable, Text, SyslogDateWritable, IntWritable> {

    private final String RFC5424 = "\\<(.*?)\\>\\d\\s(\\S*)";
    private final IntWritable INCR = new IntWritable(1);
    Pattern pattern5424 = Pattern.compile(RFC5424);

    /**
     * @param key     Input key
     * @param value   String wrapper (Text hadoop) of input string
     * @param context Context of map-reduce job
     *                <165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1
     *                myproc 8710 - - %% It's time to make the do-nuts.
     *                for https://tools.ietf.org/html/rfc5424#section-6.5
     *                parse input text (string wrapper) with regexp to get block with date-time-severity
     *                yyyy-MM-dd hh severity(name for severity)
     *                The severity can be count by getting the modulo of first block (165) by 8
     *                Text result = new Text("");
     */
    @Override
    protected void map(LongWritable key, Text value,
                       Context context) {

        log.info("Start mapping");

        SyslogDateWritable result = new SyslogDateWritable();
        Matcher matcher = pattern5424.matcher(value.toString());
        while (matcher.find()) {
            result.setSeverityWithDate(matcher.group(2).substring(0, 13)
                    + " "
                    + Severity.get(Integer.parseInt(matcher.group(1)) % 8).name());
        }
        try {
            if (result.getSeverityWithDate() != null) {
                context.write(result, INCR);
            } else {
                context.getCounter(SyslogCounter.PARSE_ERROR).increment(1);
            }
        } catch (IOException | InterruptedException e) {
            context.getCounter(SyslogCounter.MALFORMED).increment(1);
            log.error("Can't write {} to mapper context {}", value, e);
        }
        log.info("Finish mapping");
    }
}
