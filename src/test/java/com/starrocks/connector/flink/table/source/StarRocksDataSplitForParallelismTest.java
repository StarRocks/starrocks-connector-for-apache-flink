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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.QueryPlan;
public class StarRocksDataSplitForParallelismTest {
    
    @Test
    public void TestSplitData() {

        List<Long> originTableList = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);

        QueryBeXTablets qBeXTablets0 = new QueryBeXTablets("beNode0", Arrays.asList(1L, 2L, 3L, 4L));
        QueryBeXTablets qBeXTablets1 = new QueryBeXTablets("beNode1", Arrays.asList(5L, 6L, 7L));
        QueryBeXTablets qBeXTablets2 = new QueryBeXTablets("beNode2", Arrays.asList(8L, 9L, 10L));

        List<QueryBeXTablets> queryBeXTablets = new ArrayList<>();
        queryBeXTablets.add(qBeXTablets0);
        queryBeXTablets.add(qBeXTablets1);
        queryBeXTablets.add(qBeXTablets2);

        QueryInfo queryInfo = new QueryInfo(new QueryPlan(), queryBeXTablets);

        for (int parallelism = 1; parallelism <= 20; parallelism ++) {
            List<Long> actuals = new ArrayList<>();
            List<List<QueryBeXTablets>> lists = StarRocksSourceCommonFunc.splitQueryBeXTablets(parallelism, queryInfo);
            lists.forEach(total -> {
                total.forEach(beXTablets -> {
                    beXTablets.getTabletIds().forEach(tabletId -> {
                        actuals.add(tabletId);
                    });
                });
            });
            Collections.sort(actuals);
            assertArrayEquals(originTableList.toArray(), actuals.toArray());
        }
    }
}
