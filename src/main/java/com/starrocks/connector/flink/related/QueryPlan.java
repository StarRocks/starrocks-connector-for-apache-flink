package com.starrocks.connector.flink.related;

import java.util.Map;
import java.util.Objects;

public class QueryPlan {

    private int status;
    private String opaqued_query_plan;
    private Map<String, Tablet> partitions;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getOpaqued_query_plan() {
        return opaqued_query_plan;
    }

    public void setOpaqued_query_plan(String opaqued_query_plan) {
        this.opaqued_query_plan = opaqued_query_plan;
    }

    public Map<String, Tablet> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, Tablet> partitions) {
        this.partitions = partitions;
    }
}
