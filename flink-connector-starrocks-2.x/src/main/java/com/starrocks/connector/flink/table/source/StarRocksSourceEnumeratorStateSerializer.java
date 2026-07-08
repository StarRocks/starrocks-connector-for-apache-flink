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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/** Serializer for the enumerator state (a collection of unassigned splits). */
public class StarRocksSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<Collection<StarRocksSourceSplit>> {

    public static final StarRocksSourceEnumeratorStateSerializer INSTANCE =
            new StarRocksSourceEnumeratorStateSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Collection<StarRocksSourceSplit> splits) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(splits.size());
        for (StarRocksSourceSplit split : splits) {
            byte[] splitBytes = StarRocksSourceSplitSerializer.INSTANCE.serialize(split);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public Collection<StarRocksSourceSplit> deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        int size = in.readInt();
        Collection<StarRocksSourceSplit> splits = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int splitBytesLen = in.readInt();
            byte[] splitBytes = new byte[splitBytesLen];
            in.readFully(splitBytes);
            splits.add(StarRocksSourceSplitSerializer.INSTANCE.deserialize(VERSION, splitBytes));
        }
        return splits;
    }
}
