package com.starrocks.connector.flink.related;

import com.starrocks.connector.flink.exception.TSocketException;
import com.starrocks.connector.flink.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.util.List;

public class BackendClient {

    private TStarrocksExternalService.Client client;

    private final String IP;
    private final int PORT;
    private String contextId;

    public BackendClient(String ip, int port) throws TSocketException {
        this.IP = ip;
        this.PORT = port;
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        // todo: timeout
        TSocket socket = new TSocket(IP, PORT, 50000, 50000);
        try {
            socket.open();
        } catch (TTransportException e) {
            throw new TSocketException("socket not open" + e.getMessage());
        }
        TProtocol protocol = factory.getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);
    }

    public void openScanner(List<Long> tablets, String opaqued_query_plan) throws TException {
        TScanOpenParams params = new TScanOpenParams();
        params.setCluster("default_cluster");
        params.setDatabase("cjs_test");
        params.setTable("test_1");

        params.setTablet_ids(tablets);
        params.setOpaqued_query_plan(opaqued_query_plan);

        params.setBatch_size(100);
        params.setQuery_timeout(5000);
        params.setMem_limit(1024);
        params.setUser("root");
        params.setPasswd("");
        TScanOpenResult result = client.open_scanner(params);
        this.contextId = result.getContext_id();
    }

    public void getNext() throws TException {
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(this.contextId);
        params.setOffset(0);
        TScanBatchResult result = client.get_next(params);
        System.out.println(result);
    }
}
