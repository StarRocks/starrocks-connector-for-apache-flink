package com.starrocks.connector.flink.related;

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
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
import java.util.concurrent.LinkedBlockingDeque;

public class DataReader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksJdbcConnectionProvider.class);

    private TStarrocksExternalService.Client client;
    private final String IP;
    private final int PORT;
    private Schema schema;
    private String contextId;
    private int readerOffset = 0;
    private boolean readerEOS = false;

    private final LinkedBlockingDeque<List<Object>> dataQueue;

    public LinkedBlockingDeque<List<Object>> getDataQueue() {
        return dataQueue;
    }

    private ReadJobStatus status;

    private enum ReadJobStatus {
        WAITING,RUNNING,DONE
    }

    public DataReader(String ip, int port, int socketTimeout, int connectTimeout) throws StarRocksException {
        this.IP = ip;
        this.PORT = port;
        this.dataQueue = new LinkedBlockingDeque<>();
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
                        "open scanner was wrong"
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

    public boolean startToRead() throws StarRocksException {

        status = ReadJobStatus.WAITING;
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(this.contextId);
        params.setOffset(this.readerOffset);
        TScanBatchResult result;
        try {
            result = client.get_next(params);
            if (!result.eos) {
                handleResult(result);
            }
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new StarRocksException(
                        "get next was wrong"
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs()
                );
            }
        } catch (TException | InterruptedException e) {
            throw new StarRocksException(e.getMessage());
        }
        this.readerEOS = result.eos;
        if (this.readerEOS) {
            return false;
        }
        Thread thread = new Thread(() -> {
            try {
                this.continueToRead(params);
            } catch (TException | StarRocksException | InterruptedException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
            }
        });
        thread.start();
        return true;
    }

    private void continueToRead(TScanNextBatchParams params) throws TException, StarRocksException, InterruptedException {

        status = ReadJobStatus.RUNNING;
        while (!this.readerEOS) {
            params.setOffset(this.readerOffset);
            TScanBatchResult result = client.get_next(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new StarRocksException(
                        "get next was wrong"
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs()
                );
            }
            this.readerEOS = result.eos;
            if (!readerEOS) {
                handleResult(result);
            }
        }
        status = ReadJobStatus.DONE;
    }

    private void handleResult(TScanBatchResult result) throws StarRocksException, InterruptedException {
        RowBatch rowBatch = new RowBatch(result, this.schema).readArrow();
        this.readerOffset = rowBatch.getReadRowCount() + this.readerOffset;
        while (rowBatch.hasNext()) {
            List<Object> row = rowBatch.next();
            this.dataQueue.put(row);
        }
    }

    public boolean jobDone() {
        return this.status == ReadJobStatus.DONE && this.dataQueue.size() == 0;
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
