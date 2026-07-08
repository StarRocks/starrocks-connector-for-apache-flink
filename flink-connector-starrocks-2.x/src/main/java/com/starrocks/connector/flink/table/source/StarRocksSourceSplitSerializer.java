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

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link StarRocksSourceSplit}. */
public class StarRocksSourceSplitSerializer implements SimpleVersionedSerializer<StarRocksSourceSplit> {

    public static final StarRocksSourceSplitSerializer INSTANCE = new StarRocksSourceSplitSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(StarRocksSourceSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeUTF(split.splitId());
        QueryBeXTablets beXTablets = split.getBeXTablets();
        out.writeUTF(beXTablets.getBeNode());
        List<Long> tabletIds = beXTablets.getTabletIds();
        out.writeInt(tabletIds.size());
        for (Long tabletId : tabletIds) {
            out.writeLong(tabletId);
        }
        String plan = split.getOpaquedQueryPlan();
        out.writeBoolean(plan != null);
        if (plan != null) {
            out.writeUTF(plan);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public StarRocksSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        String splitId = in.readUTF();
        String beNode = in.readUTF();
        int tabletCount = in.readInt();
        List<Long> tabletIds = new ArrayList<>(tabletCount);
        for (int i = 0; i < tabletCount; i++) {
            tabletIds.add(in.readLong());
        }
        QueryBeXTablets beXTablets = new QueryBeXTablets(beNode, tabletIds);
        String plan = in.readBoolean() ? in.readUTF() : null;
        return new StarRocksSourceSplit(beXTablets, splitId, plan);
    }
}
