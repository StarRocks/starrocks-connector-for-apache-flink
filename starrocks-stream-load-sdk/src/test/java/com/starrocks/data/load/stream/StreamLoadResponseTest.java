package com.starrocks.data.load.stream;

import com.alibaba.fastjson.JSON;
import org.junit.Assert;
import org.junit.Test;


public class StreamLoadResponseTest {

    @Test
    public void testDeserialize() {
        String entityContent = "{\n" +
                "    \"TxnId\": 22736752,\n" +
                "    \"Label\": \"119d4ca5-a920-4dbb-84ad-64e062a449c5\",\n" +
                "    \"Status\": \"Success\",\n" +
                "    \"Message\": \"OK\",\n" +
                "    \"NumberTotalRows\": 93,\n" +
                "    \"NumberLoadedRows\": 93,\n" +
                "    \"NumberFilteredRows\": 0,\n" +
                "    \"NumberUnselectedRows\": 0,\n" +
                "    \"LoadBytes\": 17227,\n" +
                "    \"LoadTimeMs\": 17575,\n" +
                "    \"BeginTxnTimeMs\": 0,\n" +
                "    \"StreamLoadPlanTimeMs\": 1,\n" +
                "    \"ReadDataTimeMs\": 0,\n" +
                "    \"WriteDataTimeMs\": 17487,\n" +
                "    \"CommitAndPublishTimeMs\": 86\n" +
                "}";

        StreamLoadResponse.StreamLoadResponseBody responseBody =
                JSON.parseObject(entityContent, StreamLoadResponse.StreamLoadResponseBody.class);

        Assert.assertNotNull(responseBody.getStreamLoadPlanTimeMs());
    }

}