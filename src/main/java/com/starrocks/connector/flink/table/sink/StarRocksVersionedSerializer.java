package com.starrocks.connector.flink.table.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class StarRocksVersionedSerializer implements SimpleVersionedSerializer<StarrocksSnapshotState> {

    enum SerializerVersion {
        V1(1);
        private final int version;

        SerializerVersion(int version) {
            this.version = version;
        }

        public int getVersion() {
            return version;
        }
    }

    @Override
    public int getVersion() {
        return SerializerVersion.V1.getVersion();
    }

    @Override
    public byte[] serialize(StarrocksSnapshotState state) throws IOException {
        return JSON.toJSONBytes(state);
    }

    @Override
    public StarrocksSnapshotState deserialize(int version, byte[] serialized) throws IOException {
        return JSON.parseObject(serialized, StarrocksSnapshotState.class);
    }
}
