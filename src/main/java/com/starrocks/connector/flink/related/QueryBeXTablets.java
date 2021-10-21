package com.starrocks.connector.flink.related;

import java.io.Serializable;
import java.util.List;

public class QueryBeXTablets implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String beNode;
    private final List<Long> tabletIds;


    public QueryBeXTablets(String beNode, List<Long> tabletIds) {
        this.beNode = beNode;
        this.tabletIds = tabletIds;
    }

    public String getBeNode() {
        return beNode;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }
}
