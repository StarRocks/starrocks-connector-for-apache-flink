package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import java.util.concurrent.Future;

public interface TableRegion {

    StreamLoadTableProperties getProperties();
    String getUniqueKey();
    String getDatabase();
    String getTable();

    void setLabel(String label);
    String getLabel();

    long getCacheBytes();
    long getFlushBytes();
    StreamLoadEntityMeta getEntityMeta();
    long getLastWriteTimeMillis();

    void resetAge();
    long getAndIncrementAge();
    long getAge();

    int write(byte[] row);
    byte[] read();

    boolean testPrepare();
    boolean prepare();
    boolean flush();
    boolean cancel();

    void callback(StreamLoadResponse response);
    void callback(Throwable e);
    void complete(StreamLoadResponse response);

    void setResult(Future<?> result);
    Future<?> getResult();

    boolean isReadable();
    boolean isFlushing();
}
