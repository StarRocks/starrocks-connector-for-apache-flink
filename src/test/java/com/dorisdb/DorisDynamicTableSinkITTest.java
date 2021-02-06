package com.dorisdb;

import com.dorisdb.manager.DorisQueryVisitor;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

import mockit.Mocked;
import mockit.Expectations;

import static org.junit.Assert.assertFalse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisDynamicTableSinkITTest extends DorisSinkBaseTest {

    @Mocked
    private transient DorisQueryVisitor v;
    
    @Test
    public void testBatchSink() {
        List<Map<String, String>> meta = new ArrayList<>();
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "name");
            put("DATA_TYPE", "varchar");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "score");
            put("DATA_TYPE", "bigint");
        }});
        new Expectations(){
            {
                v.getTableColumnsMetaData();
                result = meta;
            }
        };
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(bsSettings);
        mockSuccessResponse();
        String createSQL = "CREATE TABLE USER_RESULT(" +
            "name VARCHAR," +
            "score BIGINT" +
            ") WITH ( " +
            "'connector' = 'dorisdb'," +
            "'jdbc-url'='" + OPTIONS.getJdbcUrl() + "'," +
            "'load-url'='" + String.join(";", OPTIONS.getLoadUrlList()) + "'," +
            "'database-name' = '" + OPTIONS.getDatabaseName() + "'," +
            "'table-name' = '" + OPTIONS.getTableName() + "'," +
            "'username' = '" + OPTIONS.getUsername() + "'," +
            "'password' = '" + OPTIONS.getPassword() + "'," +
            "'sink.buffer-flush.max-rows' = '" + OPTIONS.getSinkMaxRows() + "'," +
            "'sink.buffer-flush.max-bytes' = '" + OPTIONS.getSinkMaxBytes() + "'," +
            "'sink.buffer-flush.interval-ms' = '" + OPTIONS.getSinkMaxFlushInterval() + "'," +
            "'sink.buffer-flush.max-retries' = '" + OPTIONS.getSinkMaxRetries() + "'" +
            ")";
        tEnv.executeSql(createSQL);

        String exMsg = "";
        try {
            tEnv.executeSql("INSERT INTO USER_RESULT\n" +
                "VALUES ('lebron', 99), ('stephen', 99)").await();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertFalse(exMsg, exMsg.length() > 0);
    }
}
