/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dorisdb.connector.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import mockit.Expectations;

import static org.junit.Assert.assertFalse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.dorisdb.connector.flink.table.DorisSinkOptions;
import com.mysql.jdbc.Driver;

public class DorisCDCTest {
    
    @Test
    public void testCDCStreamingSink() {
        // Class<Driver> a = Driver.class;
        // SourceFunction<SourceRecord> source = MySQLSource.<SourceRecord>builder()
        //     .hostname("localhost")
        //     .port(3306)
        //     .tableList("datax_test.test9")
        //     .username("root")
        //     .password("123456")
        //     .build();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env
        //     .addSource(source)
        //     .addSink(DorisSink.sink(
        //         TableSchema.builder()
        //             .field("k1", DataTypes.TINYINT().notNull())
        //             .field("k2", DataTypes.TINYINT().notNull())
        //             .field("v1", DataTypes.CHAR(10))
        //             .field("v2", DataTypes.INT())
        //             .field("v3", DataTypes.DATE())
        //             .field("v4", DataTypes.TIMESTAMP())
        //             .field("v5", DataTypes.TIMESTAMP())
        //             .primaryKey("pri", new String[]{"k1", "k2"})
        //             .build(),
        //         DorisSinkOptions.builder()
        //             .withProperty("jdbc-url", "jdbc:mysql://172.26.92.139:8887")
        //             .withProperty("load-url", "172.26.92.139:8531")
        //             .withProperty("database-name", "aa")
        //             .withProperty("table-name", "test9")
        //             .withProperty("username", "root")
        //             .withProperty("password", "")
        //             .withProperty("sink.properties.format", "json")
        //             .withProperty("sink.properties.strip_outer_array", "true")
        //             .build(),
        //         (slots, te) -> {
        //             int b = 0;
        //         })).setParallelism(1);
        // String exMsg = "";
        // try {
        //     env.execute();
        // } catch (Exception e) {
        //     exMsg = e.getMessage();
        // }
        // assertFalse(exMsg, exMsg.length() > 0);
    }
    
    @Test
    public void testCDCSQLSink() {
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
        .useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(bsSettings);
        String createSQL = "CREATE TABLE cdc_src(" +
            "k1 tinyint not null," +
            "k2 tinyint not null," +
            "v1 char(10)," +
            "v2 int," +
            "v3 date," +
            "v4 timestamp," +
            "v5 timestamp," +
            " primary key (k1, k2) not enforced " +
            ") WITH ( " +
            "'connector' = 'mysql-cdc'," +
            "'hostname' = 'localhost', " +
            "'port' = '3306', " +
            "'username' = 'root', " +
            "'password' = '123456', " +
            "'database-name' = 'datax_test', " +
            "'table-name' = 'test9' " +
            ")";
        tEnv.executeSql(createSQL);
        Class<Driver> a = Driver.class;

        String createDstSQL = "CREATE TABLE cdc_dst(" +
            "k1 tinyint not null," +
            "k2 tinyint not null," +
            "v1 char(10)," +
            "v2 int," +
            "v3 date," +
            "v4 timestamp," +
            "v5 timestamp," +
            " primary key (k1, k2) not enforced " +
            ") WITH ( " +
            "'connector' = 'doris'," +
            "'jdbc-url'='jdbc:mysql://172.26.92.139:8887'," +
            "'load-url'='172.26.92.139:8531'," +
            "'database-name' = 'aa'," +
            "'table-name' = 'test9'," +
            "'username' = 'root'," +
            "'password' = ''," +
            "'sink.buffer-flush.interval-ms' = '3000'," +
            "'sink.properties.format' = 'json'," +
            "'sink.properties.strip_outer_array' = 'true'," +
            "'sink.properties.column_separator' = '\\x01'," +
            "'sink.properties.row_delimiter' = '\\x02'" +
            ")";
        tEnv.executeSql(createDstSQL);

        String exMsg = "";
        try {
            tEnv.executeSql("insert into cdc_dst select * from cdc_src").collect();
            Thread.sleep(2000000);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertFalse(exMsg, exMsg.length() > 0);
    }


}
