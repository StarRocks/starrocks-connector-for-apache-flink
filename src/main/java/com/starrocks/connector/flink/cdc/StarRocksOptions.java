/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starrocks.connector.flink.cdc;

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.tools.IOUtils;

import java.io.Serializable;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StarRocksOptions implements Serializable {
    private StarRocksJdbcConnectionOptions opts;
    private String tableIdentifier;

    public StarRocksOptions(String username, String password, String tableIdentifier, String jdbcUrl) {
        this.opts = new StarRocksJdbcConnectionOptions(jdbcUrl, username, password);
        this.tableIdentifier = tableIdentifier;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public StarRocksJdbcConnectionOptions getOpts() {
        return opts;
    }

    public String save() throws IllegalArgumentException {
        Properties copy = new Properties();
        return IOUtils.propsToString(copy);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String jdbcUrl;
        private String username;
        private String password;
        private String tableIdentifier;

        public Builder setTableIdentifier(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public StarRocksOptions build() {
            checkNotNull(tableIdentifier, "No tableIdentifier supplied.");
            return new StarRocksOptions(username, password, tableIdentifier, jdbcUrl);
        }
    }

}
