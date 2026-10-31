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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializer for {@link StarRocksSourceEnumeratorState}. Pending splits normally share one query plan,
 * so each distinct plan is written once and the splits refer to it.
 */
public class StarRocksSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<StarRocksSourceEnumeratorState> {

    public static final StarRocksSourceEnumeratorStateSerializer INSTANCE =
            new StarRocksSourceEnumeratorStateSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(StarRocksSourceEnumeratorState state) throws IOException {
        StarRocksSourceSplitSerializer splitSerializer = StarRocksSourceSplitSerializer.INSTANCE;
        List<String> plans = new ArrayList<>();
        Map<String, Integer> planIndexes = new HashMap<>();
        List<byte[]> splitBytes = new ArrayList<>();
        List<Integer> splitPlanIndexes = new ArrayList<>();
        for (StarRocksSourceSplit split : state.getPendingSplits()) {
            String plan = split.getOpaquedQueryPlan();
            int planIndex = -1;
            if (plan != null) {
                planIndex = planIndexes.computeIfAbsent(plan, p -> {
                    plans.add(p);
                    return plans.size() - 1;
                });
            }
            splitBytes.add(splitSerializer.serialize(
                    new StarRocksSourceSplit(split.getBeXTablets(), split.splitId(), null)));
            splitPlanIndexes.add(planIndex);
        }

        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeBoolean(state.isSplitsDiscovered());
        out.writeInt(splitSerializer.getVersion());
        out.writeInt(plans.size());
        for (String plan : plans) {
            byte[] planBytes = plan.getBytes(StandardCharsets.UTF_8);
            out.writeInt(planBytes.length);
            out.write(planBytes);
        }
        out.writeInt(splitBytes.size());
        for (int i = 0; i < splitBytes.size(); i++) {
            out.writeInt(splitPlanIndexes.get(i));
            out.writeInt(splitBytes.get(i).length);
            out.write(splitBytes.get(i));
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public StarRocksSourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown StarRocks source enumerator state version: " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        boolean splitsDiscovered = in.readBoolean();
        int splitVersion = in.readInt();
        List<String> plans = new ArrayList<>();
        int planCount = in.readInt();
        for (int i = 0; i < planCount; i++) {
            byte[] planBytes = new byte[in.readInt()];
            in.readFully(planBytes);
            plans.add(new String(planBytes, StandardCharsets.UTF_8));
        }
        int splitCount = in.readInt();
        List<StarRocksSourceSplit> splits = new ArrayList<>(splitCount);
        for (int i = 0; i < splitCount; i++) {
            int planIndex = in.readInt();
            byte[] splitBytes = new byte[in.readInt()];
            in.readFully(splitBytes);
            StarRocksSourceSplit split = StarRocksSourceSplitSerializer.INSTANCE.deserialize(splitVersion, splitBytes);
            splits.add(new StarRocksSourceSplit(
                    split.getBeXTablets(), split.splitId(), planIndex < 0 ? null : plans.get(planIndex)));
        }
        return new StarRocksSourceEnumeratorState(splitsDiscovered, splits);
    }
}
