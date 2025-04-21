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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.types.Row;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.tools.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StarRocksDynamicCachedLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicCachedLookupFunction.class);

    private final ColumnRichInfo[] filterRichInfos;
    private final StarRocksSourceOptions sourceOptions;
    private QueryInfo queryInfo;
    private final SelectColumn[] selectColumns;
    private final List<ColumnRichInfo> columnRichInfos;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    // cache for lookup data
    private Map<Row, List<RowData>> cacheMap;

    private transient long nextLoadTime;

    public StarRocksDynamicCachedLookupFunction(StarRocksSourceOptions sourceOptions,
                                          ColumnRichInfo[] filterRichInfos,
                                          List<ColumnRichInfo> columnRichInfos,
                                          SelectColumn[] selectColumns) {
        this.sourceOptions = sourceOptions;
        this.filterRichInfos = filterRichInfos;
        this.columnRichInfos = columnRichInfos;
        this.selectColumns = selectColumns;

        this.cacheMaxSize = sourceOptions.getLookupCacheMaxRows();
        this.cacheExpireMs = sourceOptions.getLookupCacheTTL();
        this.maxRetryTimes = sourceOptions.getLookupMaxRetries();

        this.cacheMap = new HashMap<>();
        this.nextLoadTime = -1L;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.info("Open lookup function. {}", EnvUtils.getGitInformation());
    }

    @Override
    public Collection<RowData> lookup(RowData rowData) {
        reloadData();
        Row keyRow = genRow(rowData);
        return cacheMap.get(keyRow);
    }

    private Row genRow(RowData rowData) {
        GenericRowData gRowData = (GenericRowData) rowData;
        Object[] keyObj = new Object[filterRichInfos.length];
        for (int i = 0; i < filterRichInfos.length; i ++) {
            keyObj[i] = gRowData.getField(filterRichInfos[i].getColumnIndexInSchema());
        }
        return Row.of(keyObj);
    }

    private void reloadData() {
        if (nextLoadTime > System.currentTimeMillis()) {
            return;
        }
        if (nextLoadTime > 0) {
            LOG.info("Lookup join cache has expired after {} (ms), reloading", this.cacheExpireMs);
        } else {
            LOG.info("Populating lookup join cache");
        }
        cacheMap.clear();

        String columns = Arrays.stream(selectColumns)
                .map(col -> "`" + col.getColumnName() + "`")
                .collect(Collectors.joining(","));
        String sql = String.format("select %s from `%s`.`%s`", columns,
                sourceOptions.getDatabaseName(), sourceOptions.getTableName());
        LOG.info("LookUpFunction SQL [{}]", sql);
        this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, sql);
        List<List<QueryBeXTablets>> lists = StarRocksSourceCommonFunc.splitQueryBeXTablets(1, queryInfo);
        cacheMap = lists.get(0).parallelStream()
                .flatMap(beXTablets -> scanBeTablets(beXTablets).stream())
                .collect(Collectors.groupingBy(this::genRow));
        nextLoadTime = System.currentTimeMillis() + this.cacheExpireMs;
    }

    private List<RowData> scanBeTablets(QueryBeXTablets beXTablets) {
        List<RowData> tmpDataList = new ArrayList<>();
        RuntimeException exception = null;
        StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(
                beXTablets.getBeNode(),
                columnRichInfos,
                selectColumns,
                sourceOptions);
        try {
            beReader.openScanner(beXTablets.getTabletIds(), queryInfo.getQueryPlan().getOpaqued_query_plan(),
                    sourceOptions);
            beReader.startToRead();
            while (beReader.hasNext()) {
                RowData row = beReader.getNext();
                tmpDataList.add(row);
            }
        } catch (Exception e) {
            LOG.error("Failed to scan tablets for BE node {}", beXTablets.getBeNode(), e);
            exception = new RuntimeException("Failed to scan tablets for BE node " + beXTablets.getBeNode(), e);
        } finally {
            try {
                beReader.close();
                LOG.info("Close reader for BE {}", beXTablets.getBeNode());
            } catch (Exception ie) {
                LOG.error("Failed to close reader for BE {}", beXTablets.getBeNode(), ie);
                if (exception == null) {
                    exception = new RuntimeException("Failed to close reader for BE node " + beXTablets.getBeNode(), ie);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
        return tmpDataList;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}