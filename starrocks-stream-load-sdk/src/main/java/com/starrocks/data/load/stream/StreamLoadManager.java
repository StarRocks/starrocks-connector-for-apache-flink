package com.starrocks.data.load.stream;

public interface StreamLoadManager {

    void init();
    void write(String uniqueKey, String database, String table, String... rows);
    void callback(StreamLoadResponse response);
    void callback(Throwable e);
    void flush();

    StreamLoadSnapshot snapshot();
    boolean prepare(StreamLoadSnapshot snapshot);
    boolean commit(StreamLoadSnapshot snapshot);
    boolean abort(StreamLoadSnapshot snapshot);
    void close();
}
