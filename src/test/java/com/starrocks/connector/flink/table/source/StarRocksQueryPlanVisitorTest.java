package com.starrocks.connector.flink.table.source;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.manager.StarRocksQueryPlanVisitor;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;

import org.junit.Test;

public class StarRocksQueryPlanVisitorTest extends StarRocksSourceBaseTest {

    private int tabletCount = 50;

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

    private void mockResonsefunc() {

        String[] beNode = new String[]{"172.0.0.1:9660", "172.0.0.2:9660", "172.0.0.3:9660"};

        Map<String, Object> respMap = new HashMap<>();
        respMap.put("opaqued_query_plan", "mockPlan");
        respMap.put("status", "200");
        Map<Integer, Object> partitionsSet = new HashMap<>();
        for (int i = 0; i < tabletCount; i ++) {
            Map<String, Object> pMap = new HashMap<>();
            pMap.put("version", 4);
            pMap.put("versionHash", 6318449679607016199L);
            pMap.put("schemaHash", 975114127);
            pMap.put("routings", new String[]{
                beNode[i%beNode.length], 
                beNode[i%beNode.length + 1 >= beNode.length ? 0 : i%beNode.length + 1]
            });
            partitionsSet.put(i, pMap);
        }
        respMap.put("partitions", partitionsSet);
        JSONObject json = new JSONObject(respMap);
        mockResonse = json.toJSONString();
    }
}
