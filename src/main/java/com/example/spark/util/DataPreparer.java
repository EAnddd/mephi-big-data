package com.example.spark.util;

import java.io.IOException;

public interface DataPreparer {
    void prepareData() throws IOException;
    void clean();
}
