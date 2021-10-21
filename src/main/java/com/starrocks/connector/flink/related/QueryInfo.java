package com.starrocks.connector.flink.related;

import java.io.Serializable;
import java.util.List;

public class QueryInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final QueryPlan queryPlan;
    private final List<QueryBeXTablets> beXTablets;

    public QueryInfo(QueryPlan queryPlan, List<QueryBeXTablets> beXTablets) {
        this.queryPlan = queryPlan;
        this.beXTablets = beXTablets;
    }


    public QueryPlan getQueryPlan() {
        return queryPlan;
    }

    public List<QueryBeXTablets> getBeXTablets() {
        return beXTablets;
    }
}
