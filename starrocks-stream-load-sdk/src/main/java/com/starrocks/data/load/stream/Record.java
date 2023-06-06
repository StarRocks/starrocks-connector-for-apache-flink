package com.starrocks.data.load.stream;

public class Record {
    private String row;
    private Meta meta;

    public Record(String row, Meta meta) {
        this.row = row;
        this.meta = meta;
    }

    @Override
    public String toString() {
        return "Record{" +
                "row=" + row +
                ", meta=" + meta.toString() +
                "}";
    }

    public String getRow() {
        return row;
    }

    public Meta getMeta() {
        return meta;
    }
}
