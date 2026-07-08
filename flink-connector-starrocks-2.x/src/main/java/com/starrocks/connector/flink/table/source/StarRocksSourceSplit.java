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

import org.apache.flink.api.connector.source.SourceSplit;

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;

/** A split representing a set of tablets to read from a StarRocks BE node. */
public class StarRocksSourceSplit implements SourceSplit {

    private final QueryBeXTablets beXTablets;
    private final String splitId;
    // opaque query plan from the FE; null for count splits
    private final String opaquedQueryPlan;

    public StarRocksSourceSplit(QueryBeXTablets beXTablets, String splitId) {
        this(beXTablets, splitId, null);
    }

    public StarRocksSourceSplit(QueryBeXTablets beXTablets, String splitId, String opaquedQueryPlan) {
        this.beXTablets = beXTablets;
        this.splitId = splitId;
        this.opaquedQueryPlan = opaquedQueryPlan;
    }

    public String getOpaquedQueryPlan() {
        return opaquedQueryPlan;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public QueryBeXTablets getBeXTablets() {
        return beXTablets;
    }
}
