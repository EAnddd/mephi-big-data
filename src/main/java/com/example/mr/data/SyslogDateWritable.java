package com.example.mr.data;

import com.example.mr.mapper.Severity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custome type class for mapper and reducer
 */
@Data
@EqualsAndHashCode
public class SyslogDateWritable implements WritableComparable<SyslogDateWritable> {
    private String severityWithDate;

    public SyslogDateWritable() {
    }

    public SyslogDateWritable(String severityWithDate) {
        this.severityWithDate = severityWithDate;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(severityWithDate);
    }

    public SyslogDateWritable prettify() {
        String[] keys = this.getSeverityWithDate().split(" ");
        return new SyslogDateWritable(keys[0] + " " + Severity.get(Integer.parseInt(keys[1])).name());
    }

    @Override
    public String toString() {
        return this.severityWithDate;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.severityWithDate = in.readLine();
    }

    @Override
    public int compareTo(SyslogDateWritable o) {
        return StringUtils.compare(this.severityWithDate, o.getSeverityWithDate());
    }
}
