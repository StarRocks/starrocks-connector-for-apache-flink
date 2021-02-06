package com.dorisdb.row;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.Test;

import mockit.Injectable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.dorisdb.DorisSinkBaseTest;

public class DorisGenericRowTransformerTest extends DorisSinkBaseTest {


    class UserInfoForTest {
        public byte age;
        public String resume;
        public String birthDate;
        public String birthDateTime;
        public BigDecimal savings;
        public short todaySteps;
        public String name;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testTransformer(@Injectable RuntimeContext runtimeCtx) {
        DorisGenericRowTransformer<UserInfoForTest> rowTransformer = new DorisGenericRowTransformer<>((slots, m) -> {
            slots[0] = m.age;
            slots[1] = m.resume;
            slots[2] = m.birthDate;
            slots[3] = m.birthDateTime;
            slots[4] = m.savings;
            slots[5] = m.todaySteps;
            slots[6] = m.name;
        });
        rowTransformer.setRuntimeContext(runtimeCtx);
        rowTransformer.setTableSchema(TABLE_SCHEMA);
        UserInfoForTest rowData = createRowData();
        String result = rowTransformer.transform(rowData);
        Map<String, Object> rMap = (Map<String, Object>)JSON.parse(result);
        assertNotNull(rMap);
        assertEquals(TABLE_SCHEMA.getFieldCount(), rMap.size());
        for (String name : TABLE_SCHEMA.getFieldNames()) {
            assertTrue(rMap.containsKey(name));
        }
    }

    private UserInfoForTest createRowData() {
        UserInfoForTest u = new UserInfoForTest();
        u.age = 88;
        u.resume = "Imposible is Nothing.";
        u.birthDate = "1979-01-01";
        u.birthDateTime = "1979-01-01 12:01:01";
        u.savings = BigDecimal.valueOf(1000000.25);
        u.todaySteps = 1024;
        u.name = "Stephen";
        return u;
    }
}
