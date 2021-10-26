package com.starrocks.connector.flink.source;

import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.thrift.TScanBatchResult;
import com.starrocks.connector.flink.thrift.TScanCloseParams;
import com.starrocks.connector.flink.thrift.TScanNextBatchParams;
import com.starrocks.connector.flink.thrift.TScanOpenParams;
import com.starrocks.connector.flink.thrift.TScanOpenResult;
import com.starrocks.connector.flink.thrift.TStarrocksExternalService;
import com.starrocks.connector.flink.thrift.TStatusCode;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;


public class DataReader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DataReader.class);

    private TStarrocksExternalService.Client client;
    private final String IP;
    private final int PORT;
    private Schema schema;
    private String contextId;
    private int readerOffset = 0;
    private boolean resultEOS = false;


    private RowBatch curRowBatch;
    private List<Object> curData;


    public DataReader(String ip, int port, int socketTimeout, int connectTimeout) throws StarRocksException {
        this.IP = ip;
        this.PORT = port;
        
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSocket socket = new TSocket(IP, PORT, socketTimeout, connectTimeout);
        try {
            socket.open();
        } catch (TTransportException e) {
            throw  new StarRocksException(e.getMessage());
        }
        TProtocol protocol = factory.getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);
    }

    public void openScanner(List<Long> tablets, String opaqued_query_plan,
                            String db, String table,
                            int batchSize, int queryTimeout, int memLimit,
                            String user, String pwd) throws StarRocksException {

        TScanOpenParams params = new TScanOpenParams();
        params.setCluster(Const.DEFAULT_CLUSTER_NAME);
        params.setDatabase(db);
        params.setTable(table);

        params.setTablet_ids(tablets);
        params.setOpaqued_query_plan(opaqued_query_plan);

        params.setBatch_size(batchSize);
        params.setQuery_timeout(queryTimeout);
        params.setMem_limit(memLimit);
        params.setUser(user);
        params.setPasswd(pwd);
        TScanOpenResult result = null;
        try {
            result = client.open_scanner(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new StarRocksException(
                        "Open scanner failed."
                        + result.getStatus().getStatus_code()
                        + result.getStatus().getError_msgs()
                );
            }
        } catch (TException e) {
            throw new StarRocksException(e.getMessage());
        }
        this.schema = Schema.genSchema(result.getSelected_columns());
        this.contextId = result.getContext_id();
    }

    public void startToRead() throws StarRocksException {

        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(this.contextId);
        params.setOffset(this.readerOffset);
        TScanBatchResult result;
        try {
            result = client.get_next(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new StarRocksException(
                        "Get next failed."
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs()
                );
            }
            this.resultEOS = result.eos;
            if (!result.eos) {
                handleResult(result);
            }
        } catch (TException | InterruptedException e) {
            throw new StarRocksException(e.getMessage());
        }
    }


    public boolean hasNext() {
        return this.curData != null;
    }

    public List<Object> getNext() throws StarRocksException {

        List<Object> preparedData = this.curData;
        this.curData = null;
        if (this.curRowBatch.hasNext()) {
            this.curData = this.getNext();
        }
        if (this.curData != null) {
            return preparedData;    
        }
        startToRead();
        return preparedData;
    }
    
    private void handleResult(TScanBatchResult result) throws StarRocksException, InterruptedException {
        RowBatch rowBatch = new RowBatch(result, this.schema).readArrow();
        this.readerOffset = rowBatch.getReadRowCount() + this.readerOffset;
        this.curRowBatch = rowBatch;
        this.curData = rowBatch.next();
    }

    public void close() throws StarRocksException {
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        try {
            this.client.close_scanner(tScanCloseParams);
        } catch (TException e) {
            throw new StarRocksException(e.getMessage());
        }
    }
}
