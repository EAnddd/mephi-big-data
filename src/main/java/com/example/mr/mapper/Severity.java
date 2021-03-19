package com.example.mr.mapper;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Severity level according to RFC5424 (Syslog protocol)
 */
public enum Severity {
    Emergency(0),
    Alert(1),
    Critical(2),
    Error(3),
    Warning(4),
    Notice(5),
    Informational(6),
    Debug(7);

    private final int code;
    private static final Map<Integer,Severity> lookup = new HashMap<>();

    static {
        for(Severity s : EnumSet.allOf(Severity.class))
            lookup.put(s.getCode(), s);
    }

    Severity(int code) {
        this.code = code;
    }

    public int getCode() { return code; }

    /**
     * @param code Integer code of severity level
     * @return String code of severity level
     */
    public static Severity get(int code) {
        return lookup.get(code);
    }
}
