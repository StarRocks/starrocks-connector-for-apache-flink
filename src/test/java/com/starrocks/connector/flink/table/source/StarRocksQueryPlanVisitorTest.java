package com.starrocks.connector.flink.table.source;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.manager.StarRocksQueryPlanVisitor;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;

import org.junit.Test;

public class StarRocksQueryPlanVisitorTest extends StarRocksSourceBaseTest {


    @Test
    public void testGetQueryPlan() throws IOException {
        mockResonsefunc();
        StarRocksQueryPlanVisitor visitor = new StarRocksQueryPlanVisitor(OPTIONS);
        QueryInfo queryInfo = visitor.getQueryInfo(querySQL);
        List<Long> tabletsList = new ArrayList<>();
        List<Integer> countList = new ArrayList<>();
        queryInfo.getBeXTablets().forEach(beXTablets -> {
            tabletsList.addAll(beXTablets.getTabletIds());
            countList.add(beXTablets.getTabletIds().size());
        });

        Collections.sort(tabletsList);
        Collections.sort(countList);
        assertTrue((countList.get(countList.size() - 1) - countList.get(0)) <= 1);
        for (int i = 0; i < tabletCount; i ++) {
            assertTrue(i == tabletsList.get(i));
        }
    }
}
