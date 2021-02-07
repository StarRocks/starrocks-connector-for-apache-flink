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

package com.dorisdb.connector.flink.table;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.annotation.Internal;

/**
 * Doris sink semantic Enum.
 */
@Internal
public enum DorisSinkSemantic {
    EXACTLY_ONCE("exactly-once"), AT_LEAST_ONCE("at-least-once");

    private String name;

    private DorisSinkSemantic(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static DorisSinkSemantic fromName(String n) {
        List<DorisSinkSemantic> rs = Arrays.stream(DorisSinkSemantic.values()).filter(v -> v.getName().equals(n))
                .collect(Collectors.toList());
        return rs.isEmpty() ? null : rs.get(0);
    }
}
