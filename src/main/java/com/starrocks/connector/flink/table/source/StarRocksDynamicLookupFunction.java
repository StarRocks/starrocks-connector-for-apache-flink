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
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.tools.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StarRocksDynamicLookupFunction extends TableFunction<RowData> {
    
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicLookupFunction.class);
    
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

    public StarRocksDynamicLookupFunction(StarRocksSourceOptions sourceOptions, 
                                          ColumnRichInfo[] filterRichInfos,
                                          List<ColumnRichInfo> columnRichInfos,
                                          SelectColumn[] selectColumns
                                          ) {
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

    public void eval(Object... keys) {
        reloadData();
        Row keyRow = Row.of(keys);
        List<RowData> curList = cacheMap.get(keyRow);
        if (curList != null) {
            curList.parallelStream().forEach(this::collect);
        }
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
        
        StringBuilder sqlSb = new StringBuilder("select * from ");
        sqlSb.append("`").append(sourceOptions.getDatabaseName()).append("`");
        sqlSb.append(".");
        sqlSb.append("`" + sourceOptions.getTableName() + "`");
        LOG.info("LookUpFunction SQL [{}]", sqlSb.toString());
        this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, sqlSb.toString());
        List<List<QueryBeXTablets>> lists = StarRocksSourceCommonFunc.splitQueryBeXTablets(1, queryInfo);
        cacheMap = lists.get(0).parallelStream()
                .flatMap(beXTablets -> scanBeTablets(beXTablets).stream())
                .collect(Collectors.groupingBy(row -> {
                    GenericRowData gRowData = (GenericRowData)row;
                    Object[] keyObj = new Object[filterRichInfos.length];
                    for (int i = 0; i < filterRichInfos.length; i ++) {
                        keyObj[i] = gRowData.getField(filterRichInfos[i].getColumnIndexInSchema());
                    }
                    return Row.of(keyObj);
                }));
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
