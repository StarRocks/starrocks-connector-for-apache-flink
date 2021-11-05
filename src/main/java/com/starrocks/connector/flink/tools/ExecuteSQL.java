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

package com.starrocks.connector.flink.tools;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ExecuteSQL {
    public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (!params.has("f") || Strings.isNullOrEmpty(params.get("f"))) {
            throw new IllegalArgumentException("No sql file specified.");
        }
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(bsSettings);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String[] sqls = (String.join("\n", env.readTextFile(params.get("f")).collect()) + "\n").split(";\\s*\n");
        for (String sql : sqls) {
            System.out.println(String.format("Executing SQL: \n%s\n", sql));
            tEnv.executeSql(sql);
        }
    }
}
