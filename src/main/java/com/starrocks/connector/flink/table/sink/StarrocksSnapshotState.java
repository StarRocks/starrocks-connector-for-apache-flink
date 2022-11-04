package com.starrocks.connector.flink.table.sink;

import com.starrocks.connector.flink.tools.SRFCUtils;
import com.starrocks.data.load.stream.StreamLoadSnapshot;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class StarrocksSnapshotState implements Serializable {
    private String version;
    private Map<Long, List<StreamLoadSnapshot>> data;

    public static StarrocksSnapshotState of(Map<Long, List<StreamLoadSnapshot>> data) {
        StarrocksSnapshotState starrocksSnapshotState = new StarrocksSnapshotState();
        starrocksSnapshotState.version = SRFCUtils.getSRFCVersion();
        starrocksSnapshotState.data = data;
        return starrocksSnapshotState;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<Long, List<StreamLoadSnapshot>> getData() {
        return data;
    }

    public void setData(Map<Long, List<StreamLoadSnapshot>> data) {
        this.data = data;
    }
}
