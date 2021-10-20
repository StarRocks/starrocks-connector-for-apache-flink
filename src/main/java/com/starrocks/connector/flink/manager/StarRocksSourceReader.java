package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.exception.TSocketException;
import com.starrocks.connector.flink.related.BackendClient;
import com.starrocks.connector.flink.thrift.TScanOpenParams;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class StarRocksSourceReader implements Serializable {

    private final Map<String, Set<Long>> beXTables;
    private final String opaquedQueryPlan;

    public StarRocksSourceReader(Map<String, Set<Long>> beXTables, String opaquedQueryPlan) {
        this.beXTables = beXTables;
        this.opaquedQueryPlan = opaquedQueryPlan;
    }

    public void startRead() {
        Thread readThread = new Thread(() -> {
            this.beXTables.forEach((be, tablets) -> {
                String beNode[] = be.split(":");
                String ip = beNode[0];
                int port = Integer.parseInt(beNode[1]);
                BackendClient backendClient = null;
                try {
                    backendClient = new BackendClient(ip, port);
                } catch (TSocketException | TTransportException e) {
                    e.printStackTrace();
                }
                try {
                    backendClient.openScanner(new ArrayList<>(tablets), this.opaquedQueryPlan);
                } catch (TException e) {
                    e.printStackTrace();
                }
                try {
                    backendClient.getNext();
                } catch (TException e) {
                    e.printStackTrace();
                }
            });
        });
        readThread.start();
        try {
            Thread.sleep(1000 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
