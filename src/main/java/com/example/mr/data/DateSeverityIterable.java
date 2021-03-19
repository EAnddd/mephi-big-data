package com.example.mr.data;

import java.util.Iterator;

public class DateSeverityIterable implements Iterable<DateSeverity>{
    private DateSeverity[] dateSeverities;
    private int currentSize;

    public DateSeverityIterable(DateSeverity[] dateSeverities) {
        this.dateSeverities = dateSeverities;
        this.currentSize = dateSeverities.length;
    }
    @Override
    public Iterator<DateSeverity> iterator() {
        return new Iterator<DateSeverity>() {
            private int curr = 0;
            @Override
            public boolean hasNext() {
                return curr < currentSize && dateSeverities[curr] != null;
            }

            @Override
            public DateSeverity next() {
                return dateSeverities[curr++];
            }
        };
    }
}
