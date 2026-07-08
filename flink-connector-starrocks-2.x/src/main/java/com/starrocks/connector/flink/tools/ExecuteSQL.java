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

import com.google.common.base.Strings;
import org.apache.flink.util.MultipleParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExecuteSQL {
    public static void main(String[] args) throws Exception {
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (!params.has("f") || Strings.isNullOrEmpty(params.get("f"))) {
            throw new IllegalArgumentException("No sql file specified.");
        }
        Path path = Paths.get(params.get("f"));
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(bsSettings);
        String[] sqls = (String.join("\n", Files.readAllLines(path)) + "\n").split(";\\s*\n");
        for (String sql : sqls) {
            System.out.println(String.format("Executing SQL: \n%s\n", sql));
            tEnv.executeSql(sql);
        }
    }
}
