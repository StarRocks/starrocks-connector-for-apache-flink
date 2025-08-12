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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.junit.Assert;
import org.junit.Test;

public class StreamLoadPropertiesSanitizeFlagTest {

    @Test
    public void testSanitizeFlagDefaultsToFalse() {
        StreamLoadProperties props = StreamLoadProperties.builder()
                .jdbcUrl("jdbc:mysql://localhost:9030")
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("")
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database("db").table("tbl").build())
                .build();
        Assert.assertFalse(props.isSanitizeErrorLog());
    }

    @Test
    public void testSanitizeFlagTrue() {
        StreamLoadProperties props = StreamLoadProperties.builder()
                .jdbcUrl("jdbc:mysql://localhost:9030")
                .loadUrls("http://localhost:8030")
                .username("root")
                .password("")
                .defaultTableProperties(StreamLoadTableProperties.builder()
                        .database("db").table("tbl").build())
                .sanitizeErrorLog(true)
                .build();
        Assert.assertTrue(props.isSanitizeErrorLog());
    }
}


