package com.starrocks.data.load.stream.http;

import java.io.Serializable;

public class StreamLoadEntityMeta implements Serializable {

    public static final StreamLoadEntityMeta CHUNK_ENTITY_META = new StreamLoadEntityMeta(-1, -1);

    private long bytes;
    private long rows;

    public StreamLoadEntityMeta(long bytes, long rows) {
        this.bytes = bytes;
        this.rows = rows;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }
}
