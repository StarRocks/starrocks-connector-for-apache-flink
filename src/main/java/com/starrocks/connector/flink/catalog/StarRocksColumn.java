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

package com.starrocks.connector.flink.catalog;

import java.io.Serializable;
import javax.annotation.Nullable;

/** Describe a column of StarRocks table. */
public class StarRocksColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    // Index of column in table (starting at 0)
    private final int ordinalPosition;
    private final String type;
    @Nullable
    private final String key;
    @Nullable
    private final Integer size;
    @Nullable
    private final Integer scale;
    @Nullable
    private final String defaultValue;
    private final boolean isNullable;
    @Nullable
    private final String comment;

    private StarRocksColumn(String name, int ordinalPosition, String type, @Nullable String key,
                            @Nullable Integer size, @Nullable Integer scale, @Nullable String defaultValue,
                            boolean isNullable, @Nullable String comment) {
        this.name = name;
        this.ordinalPosition = ordinalPosition;
        this.type = type;
        this.key = key;
        this.size = size;
        this.scale = scale;
        this.defaultValue = defaultValue;
        this.isNullable = isNullable;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getType() {
        return type;
    }

    @Nullable
    public String getKey() {
        return key;
    }

    @Nullable
    public Integer getSize() {
        return size;
    }

    @Nullable
    public Integer getScale() {
        return scale;
    }

    @Nullable
    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isNullable() {
        return isNullable;
    }

    @Nullable
    public String getComment() {
        return comment;
    }

    public static class Builder {

        private String name;
        private int ordinalPosition;
        private String type;
        private String key;
        private Integer size;
        private Integer scale;
        private String defaultValue;
        boolean isNullable;
        private String comment;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setOrdinalPosition(int ordinalPosition) {
            this.ordinalPosition = ordinalPosition;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setKey(String key) {
            this.key = key;
            return this;
        }

        public Builder setSize(Integer size) {
            this.size = size;
            return this;
        }

        public Builder setScale(Integer scale) {
            this.scale = scale;
            return this;
        }

        public Builder setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder setNullable(boolean nullable) {
            isNullable = nullable;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public StarRocksColumn build() {
            return new StarRocksColumn(name, ordinalPosition, type, key, size, scale,
                    defaultValue, isNullable, comment);
        }
    }
}