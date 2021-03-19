package com.example.mr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class SyslogDataUtil {
    private static final String RFC5424 = "<%d>1 %s %s";

    public static void prerareData(String filePath, String output, int lineQuantity) throws IOException {
        DateTimeFormatter formatter =
                DateTimeFormatter.ISO_INSTANT;
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path filenamePath = new Path(filePath);
        Path outputPath = new Path(output);
        if (fs.exists(filenamePath)) {
            fs.delete(filenamePath, true);
        }

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        int i = 0;
        int randomSeconds;
        try (FSDataOutputStream fin = fs.create(filenamePath)) {
            while (i < lineQuantity) {
                i++;
                randomSeconds = new Random().nextInt(3600 * 24);
                fin.writeBytes(String.format(RFC5424,
                        (int) (Math.random() * 20) + 8,
                        LocalDateTime.now().minusSeconds(randomSeconds) + "Z",
                        "That's test message") + "\n");
                if (i % 100 == 0) {
                    fin.writeUTF("Breaking bad");
                    fin.writeUTF("\n");
                }
            }
        }
    }
}
