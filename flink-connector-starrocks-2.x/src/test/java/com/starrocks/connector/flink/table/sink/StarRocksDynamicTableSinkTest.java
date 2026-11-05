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

package com.starrocks.connector.flink.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;

public class StarRocksDynamicTableSinkTest {

    private static final ResolvedSchema KEYED = new ResolvedSchema(
            Arrays.asList(Column.physical("id", DataTypes.INT().notNull()), Column.physical("v", DataTypes.STRING())),
            Collections.emptyList(),
            UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final ResolvedSchema UNKEYED = ResolvedSchema.of(
            Column.physical("id", DataTypes.INT()), Column.physical("v", DataTypes.STRING()));

    @Test
    public void testKeyedSinkIgnoringUpdateBeforeIsUpsert() {
        assertEquals(EnumSet.of(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE),
                changelogMode(KEYED, true, ChangelogMode.all()).getContainedKinds());
    }

    @Test
    public void testKeyedSinkKeepingUpdateBeforeGetsTheRequestedMode() {
        assertEquals(ChangelogMode.all().getContainedKinds(),
                changelogMode(KEYED, false, ChangelogMode.all()).getContainedKinds());
    }

    @Test
    public void testUnkeyedSinkGetsTheRequestedMode() {
        assertEquals(ChangelogMode.all().getContainedKinds(),
                changelogMode(UNKEYED, true, ChangelogMode.all()).getContainedKinds());
    }

    @Test
    public void testInsertOnlyRequestStaysInsertOnly() {
        assertEquals(EnumSet.of(RowKind.INSERT),
                changelogMode(KEYED, true, ChangelogMode.insertOnly()).getContainedKinds());
    }

    private static ChangelogMode changelogMode(ResolvedSchema schema, boolean ignoreUpdateBefore,
                                               ChangelogMode requested) {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "db");
        conf.set(StarRocksSinkOptions.TABLE_NAME, "tbl");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        conf.set(StarRocksSinkOptions.SINK_IGNORE_UPDATE_BEFORE, ignoreUpdateBefore);
        StarRocksSinkOptions options = new StarRocksSinkOptions(conf, conf.toMap());
        return new StarRocksDynamicTableSink(options, schema).getChangelogMode(requested);
    }
}
