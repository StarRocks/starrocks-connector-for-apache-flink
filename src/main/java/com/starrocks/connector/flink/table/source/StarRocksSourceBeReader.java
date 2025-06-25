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

import com.starrocks.connector.flink.row.source.ArrowFieldConverter;
import com.starrocks.connector.flink.row.source.StarRocksSourceFlinkRows;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.shade.org.apache.thrift.TException;
import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.protocol.TProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.shade.org.apache.thrift.transport.TTransportException;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TStarrocksExternalService;
import com.starrocks.thrift.TStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StarRocksSourceBeReader implements StarRocksSourceDataReader, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSourceBeReader.class);

    private TStarrocksExternalService.Client client;
    private final String IP;
    private final int PORT;
    private final List<ColumnRichInfo> columnRichInfos;
    private final SelectColumn[] selectColumns;
    private String contextId;
    private int readerOffset = 0;

    private StarRocksSourceFlinkRows curFlinkRows;
    private GenericRowData curData;

    // Lazily initialized
    private final List<ArrowFieldConverter> fieldConverters = new ArrayList<>();

    public StarRocksSourceBeReader(String beNodeInfo,
                                   List<ColumnRichInfo> columnRichInfos,
                                   SelectColumn[] selectColumns,
                                   StarRocksSourceOptions sourceOptions) {
        if (sourceOptions.getBeHostMappingList().length() > 0) {
            String list = sourceOptions.getBeHostMappingList();
            Map<String, String> mappingMap = new HashMap<>();
            String[] beHostMappingInfos = list.split(";");
            for (String beHostMappingInfo : beHostMappingInfos) {
                String[] mapping = beHostMappingInfo.split(",");
                mappingMap.put(mapping[1].trim(), mapping[0].trim());
            }
            if (!mappingMap.containsKey(beNodeInfo)) {
                throw new RuntimeException("Not find be node info from the be port mappping list");
            }
            beNodeInfo = mappingMap.get(beNodeInfo);
            LOG.info("query data from be by using be-hostname {}", beNodeInfo);
        } else {
            LOG.info("query data from be by using be-ip {}", beNodeInfo);
        }
        String[] beNode = beNodeInfo.split(":");
        String ip = beNode[0].trim();
        int port = Integer.parseInt(beNode[1].trim());
        this.IP = ip;
        this.PORT = port;
        this.columnRichInfos = columnRichInfos;
        this.selectColumns = selectColumns;
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSocket socket = new TSocket(IP, PORT, sourceOptions.getConnectTimeoutMs(), sourceOptions.getConnectTimeoutMs());
        try {
            socket.open();
        } catch (TTransportException e) {
            socket.close();
            throw new RuntimeException("Failed to create brpc source:" + e.getMessage());
        }
        TProtocol protocol = factory.getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);   
    }

    public void openScanner(List<Long> tablets, String opaqued_query_plan, StarRocksSourceOptions sourceOptions) {
        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(tablets);
        params.setOpaqued_query_plan(opaqued_query_plan);
        params.setCluster("default_cluster");
        params.setDatabase(sourceOptions.getDatabaseName());
        params.setTable(sourceOptions.getTableName());
        params.setUser(sourceOptions.getUsername());
        params.setPasswd(sourceOptions.getPassword());
        params.setBatch_size(sourceOptions.getBatchRows());
        if (sourceOptions.getProperties() != null ) {
            params.setProperties(sourceOptions.getProperties());    
        }
        // params.setLimit(sourceOptions.getLimit());
        short keepAliveMin = (short) Math.min(Short.MAX_VALUE, sourceOptions.getKeepAliveMin());
        params.setKeep_alive_min(keepAliveMin);
        params.setQuery_timeout(sourceOptions.getQueryTimeout());
        params.setMem_limit(sourceOptions.getMemLimit());
        LOG.info("open Scan params.mem_limit {} B", params.getMem_limit());
        LOG.info("open Scan params.keep-alive-min {} min", params.getKeep_alive_min());
        TScanOpenResult result = null;
        try {
            result = client.open_scanner(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new RuntimeException(
                        "Failed to open scanner."
                        + result.getStatus().getStatus_code()
                        + result.getStatus().getError_msgs()
                );
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to open scanner." + e.getMessage());
        }
        this.contextId = result.getContext_id();
        LOG.info("Open scanner for {}:{} with context id {}, and there are {} tablets {}",
                IP, PORT, contextId, tablets.size(), tablets);
    }

    public void startToRead() {
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(this.contextId);
        params.setOffset(this.readerOffset);
        TScanBatchResult result;
        try {
            result = client.get_next(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new RuntimeException(
                        "Failed to get next from be -> ip:[" + IP + "] " 
                                + result.getStatus().getStatus_code() + " msg:" + result.getStatus().getError_msgs()
                );
            }
            if (!result.eos) {
                handleResult(result);
            }
        } catch (TException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        return this.curData != null;
    }

    @Override
    public GenericRowData getNext() {
        GenericRowData preparedData = this.curData;
        this.curData = null;
        if (this.curFlinkRows.hasNext()) {
            this.curData = curFlinkRows.next();
        }
        if (this.curData != null) {
            return preparedData;    
        }
        startToRead();
        return preparedData;
    }
    
    private void handleResult(TScanBatchResult result) {
        try {
            curFlinkRows = new StarRocksSourceFlinkRows(result, columnRichInfos, selectColumns);
            curFlinkRows.init(fieldConverters);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        } 
        this.readerOffset = curFlinkRows.getReadRowCount() + this.readerOffset;
        this.curData = curFlinkRows.next();
    }

    @Override
    public void close() {
        LOG.info("Close reader for {}:{} with context id {}", IP, PORT, contextId);
        if (contextId == null) {
            // not opened yet
            return;
        }
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        for (int i = 0; i < 3; i++) {
            try {
                this.client.close_scanner(tScanCloseParams);
                break;
            } catch (Exception e) {
                LOG.error("Failed to close reader {}:{} with context id {}, retries: {}", IP, PORT, contextId, i, e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("Waiting for closing is interrupted, reader {}:{} with context id {}", IP, PORT, contextId);
                break;
            }
        }
    }
}
