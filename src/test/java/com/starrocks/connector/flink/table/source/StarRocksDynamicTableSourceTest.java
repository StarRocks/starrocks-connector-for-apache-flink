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

package com.starrocks.connector.flink.table.source;

import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.table.source.struct.PushDownHolder;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.junit.Assert.assertEquals;

public class StarRocksDynamicTableSourceTest extends StarRocksSourceBaseTest {


    StarRocksDynamicTableSource dynamicTableSource;
    PushDownHolder pushDownHolder;

    @Before
    public void init() {
        pushDownHolder = new PushDownHolder();
        dynamicTableSource = new StarRocksDynamicTableSource(OPTIONS, TABLE_SCHEMA, pushDownHolder,null,null);
    }

    @Test
    public void testApplyProjection() {
        dynamicTableSource.applyProjection(PROJECTION_ARRAY);
        assertEquals("char_1, int_1", pushDownHolder.getColumns());

        for (int i = 0; i < SELECT_COLUMNS.length; i ++) {
            assertEquals(SELECT_COLUMNS[i].getColumnIndexInFlinkTable(), pushDownHolder.getSelectColumns()[i].getColumnIndexInFlinkTable());
            assertEquals(SELECT_COLUMNS[i].getColumnName(), pushDownHolder.getSelectColumns()[i].getColumnName());
        }
        assertEquals(StarRocksSourceQueryType.QuerySomeColumns, pushDownHolder.getQueryType());

        dynamicTableSource.applyProjection(PROJECTION_ARRAY_NULL);
        assertEquals(StarRocksSourceQueryType.QueryCount, pushDownHolder.getQueryType());
    }

    @Test
    public void testFilter() {

        String filter;

        ResolvedExpression c9Ref = new FieldReferenceExpression("c6", DataTypes.STRING(), 0, 2);
        ResolvedExpression c9CharLength = new CallExpression(BuiltInFunctionDefinitions.CHAR_LENGTH, Collections.singletonList(c9Ref), DataTypes.INT());
        ResolvedExpression c9Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.LESS_THAN,
                        Arrays.asList(c9CharLength, valueLiteral(10)),
                        DataTypes.BOOLEAN());
        dynamicTableSource.applyFilters(Collections.singletonList(c9Exp));
        filter = pushDownHolder.getFilter();
        Assert.assertTrue(filter.isEmpty());

        ResolvedExpression c5Ref = new FieldReferenceExpression("c5", DataTypes.TIMESTAMP(), 0, 2);
        ResolvedExpression c5Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(c5Ref, valueLiteral("2022-1-22 00:00:00")),
                        DataTypes.BOOLEAN());
        dynamicTableSource.applyFilters(Arrays.asList(c5Exp));
        filter = pushDownHolder.getFilter();
        assertEquals("(c5 = '2022-1-22 00:00:00')", filter);

        ResolvedExpression c4Ref = new FieldReferenceExpression("c4", DataTypes.DATE(), 0, 2);
        ResolvedExpression c4Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(c4Ref, valueLiteral("2022-1-22")),
                        DataTypes.BOOLEAN());
        dynamicTableSource.applyFilters(Collections.singletonList(c4Exp));
        filter = pushDownHolder.getFilter();
        assertEquals("(c4 = '2022-1-22')", filter);

        ResolvedExpression c3Ref = new FieldReferenceExpression("c3", DataTypes.BOOLEAN(), 0, 2);
        ResolvedExpression c3Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(c3Ref, valueLiteral(true)),
                        DataTypes.BOOLEAN());
        dynamicTableSource.applyFilters(Collections.singletonList(c3Exp));
        filter = pushDownHolder.getFilter();
        assertEquals("(c3 = true)", filter);

        ResolvedExpression c2Ref = new FieldReferenceExpression("c2", DataTypes.INT(), 0, 2);
        ResolvedExpression c2Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(c2Ref, valueLiteral(2)),
                        DataTypes.BOOLEAN());

        ResolvedExpression c1Ref = new FieldReferenceExpression("c1", DataTypes.INT(), 0, 2);
        ResolvedExpression c1Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(c1Ref, valueLiteral(1)),
                        DataTypes.BOOLEAN());

        dynamicTableSource.applyFilters(Arrays.asList(c1Exp,
            new CallExpression(
                BuiltInFunctionDefinitions.NOT_EQUALS,
                Arrays.asList(c1Ref, valueLiteral(1)),
                DataTypes.BOOLEAN()),
            new CallExpression(
                BuiltInFunctionDefinitions.GREATER_THAN,
                Arrays.asList(c1Ref, valueLiteral(1)),
                DataTypes.BOOLEAN()),
            new CallExpression(
                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                Arrays.asList(c1Ref, valueLiteral(1)),
                DataTypes.BOOLEAN()),
            new CallExpression(
                BuiltInFunctionDefinitions.LESS_THAN,
                Arrays.asList(c1Ref, valueLiteral(1)),
                DataTypes.BOOLEAN()),
            new CallExpression(
                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                Arrays.asList(c1Ref, valueLiteral(1)),
                DataTypes.BOOLEAN())
        ));
        filter = pushDownHolder.getFilter();
        assertEquals("(c1 = 1) and (c1 <> 1) and (c1 > 1) and (c1 >= 1) and (c1 < 1) and (c1 <= 1)", filter);

        dynamicTableSource.applyFilters(Arrays.asList(c1Exp, c2Exp));
        filter = pushDownHolder.getFilter();
        assertEquals("(c1 = 1) and (c2 = 2)", filter);


        dynamicTableSource.applyFilters(Arrays.asList(new CallExpression(BuiltInFunctionDefinitions.OR, Arrays.asList(c1Exp, c3Exp), DataTypes.BOOLEAN())));
        filter = pushDownHolder.getFilter();
        assertEquals("((c1 = 1) or (c3 = true))", filter);


        ResolvedExpression c6Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.LIKE,
                        Arrays.asList(c1Ref, valueLiteral(1)),
                        DataTypes.BOOLEAN());
        try {
            dynamicTableSource.applyFilters(Collections.singletonList(c6Exp));
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("Not support filter -> [like]", e.getMessage());
        }

        ResolvedExpression c7Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.IN,
                        Arrays.asList(c1Ref, valueLiteral(1)),
                        DataTypes.BOOLEAN());
        try {
            dynamicTableSource.applyFilters(Collections.singletonList(c7Exp));
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("Not support filter -> [in]", e.getMessage());
        }

        ResolvedExpression c8Exp =
                new CallExpression(
                        BuiltInFunctionDefinitions.BETWEEN,
                        Arrays.asList(c1Ref, valueLiteral(1)),
                        DataTypes.BOOLEAN());
        try {
            dynamicTableSource.applyFilters(Collections.singletonList(c8Exp));
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("Not support filter -> [between]", e.getMessage());
        }
    }

    @Test
    public void test() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment env = TableEnvironment.create(settings);
//        env.explainSql("create table HTWSource (item_key string, vehicle_id string, item_type string, item_value string, modify_time timestamp) WITH ('connector' = 'datagen')");
        env.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH ('connector' = 'datagen')");
        env.executeSql("CREATE TABLE HTWSource (`item_key` BIGINT, vehicle_id STRING, item_type STRING, item_value STRING, modify_time STRING) WITH ('connector' = 'datagen')");
        System.out.println(env.explainSql(
                "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'"));

        System.out.println(env.explainSql("select item_key,\n" +
                "       vehicle_id,\n" +
                "      item_type,item_value,modify_time\n" +
                "from HTWSource \n" +
                "where CHAR_LENGTH(vehicle_id) < 10"));

        List<String> s = Collections.singletonList("a");

        System.out.println(String.join(" is not null", s));
    }
}
