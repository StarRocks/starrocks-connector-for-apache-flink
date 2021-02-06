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

package com.dorisdb.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import java.util.HashSet;
import java.util.Set;

public class DorisDynamicTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DorisSinkOptions.SINK_PROPERTIES_PREFIX);
        ReadableConfig options = helper.getOptions();
        // validate some special properties
        DorisSinkOptions sinkOptions = new DorisSinkOptions(options, context.getCatalogTable().getOptions());
        sinkOptions.validate();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new DorisDynamicTableSink(sinkOptions, physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return "dorisdb";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(DorisSinkOptions.JDBC_URL);
        requiredOptions.add(DorisSinkOptions.LOAD_URL);
        requiredOptions.add(DorisSinkOptions.DATABASE_NAME);
        requiredOptions.add(DorisSinkOptions.TABLE_NAME);
        requiredOptions.add(DorisSinkOptions.USERNAME);
        requiredOptions.add(DorisSinkOptions.PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(DorisSinkOptions.SINK_BATCH_MAX_SIZE);
        optionalOptions.add(DorisSinkOptions.SINK_BATCH_MAX_ROWS);
        optionalOptions.add(DorisSinkOptions.SINK_BATCH_FLUSH_INTERVAL);
        optionalOptions.add(DorisSinkOptions.SINK_BATCH_MAX_RETRIES);
        optionalOptions.add(DorisSinkOptions.SINK_SEMANTIC);
        return optionalOptions;
    }
}
