package com.example.spark.util;

import java.io.IOException;

public interface DataPreparer {
    void prepareData(int lineQuantity) throws IOException;
    void clean();
}
