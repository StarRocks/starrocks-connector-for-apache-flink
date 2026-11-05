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

import java.util.List;

/** Checkpointed state of {@link StarRocksSourceEnumerator}. */
public class StarRocksSourceEnumeratorState {

    // true once the query plan has been turned into splits, so a restore never plans again
    private final boolean splitsDiscovered;
    // splits not assigned yet; readers checkpoint the splits they own themselves
    private final List<StarRocksSourceSplit> pendingSplits;

    public StarRocksSourceEnumeratorState(boolean splitsDiscovered, List<StarRocksSourceSplit> pendingSplits) {
        this.splitsDiscovered = splitsDiscovered;
        this.pendingSplits = pendingSplits;
    }

    public boolean isSplitsDiscovered() {
        return splitsDiscovered;
    }

    public List<StarRocksSourceSplit> getPendingSplits() {
        return pendingSplits;
    }
}
