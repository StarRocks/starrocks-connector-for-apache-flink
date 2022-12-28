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

package com.starrocks.connector.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.Serializable;
import java.util.List;

public class StarRocksOptions implements Serializable {

    public static final String IDENTIFIER = "starrocks";


    // required Options
    public static final ConfigOption<List<String>> FE_NODES = ConfigOptions.key("fe-nodes")
            .stringType().asList().noDefaultValue().withDescription("Hosts of the fe node like: `fe_ip1:http_port,fe_ip2:http_port...`.");

    public static final ConfigOption<String> JDBC_URL = ConfigOptions.key("jdbc-url")
            .stringType().noDefaultValue().withDescription("Host of the stream load like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.");

    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType().noDefaultValue().withDescription("StarRocks user name.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType().noDefaultValue().withDescription("StarRocks user password.");

    public static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name")
            .stringType().noDefaultValue().withDescription("Database name");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table-name")
            .stringType().noDefaultValue().withDescription("Table name");
}
