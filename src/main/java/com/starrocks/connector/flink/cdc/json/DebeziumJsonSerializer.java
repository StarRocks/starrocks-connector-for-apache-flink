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
package com.starrocks.connector.flink.cdc.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.starrocks.connector.flink.cdc.StarRocksOptions;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DebeziumJsonSerializer implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumJsonSerializer.class);
    private static final String OP_READ = "r"; // snapshot read
    private static final String OP_CREATE = "c"; // insert
    private static final String OP_UPDATE = "u"; // update
    private static final String OP_DELETE = "d"; // delete

    public static final String INVALID_RESULT = "invalid result";
    private static final String STARROCKS_DELETE_SIGN = "__op";

    public static final String EXECUTE_DDL = "ALTER TABLE %s %s COLUMN %s %s"; //alter table tbl add cloumn aca int
    private static final String addDropDDLRegex = "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s]+))?.*";
    private final Pattern addDropDDLPattern;
    private StarRocksOptions starRocksOptions;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String database;
    private String table;
    //table name of the cdc upstream, format is db.tbl
    private String sourceTableName;

    public DebeziumJsonSerializer(StarRocksOptions starRocksOptions, Pattern pattern, String sourceTableName) {
        this.starRocksOptions = starRocksOptions;
        this.addDropDDLPattern = pattern == null ? Pattern.compile(addDropDDLRegex, Pattern.CASE_INSENSITIVE) : pattern;
        String[] tableInfo = starRocksOptions.getTableIdentifier().split("\\.");
        this.database = tableInfo[0];
        this.table = tableInfo[1];
        this.sourceTableName = sourceTableName;
        // Prevent loss of decimal data precision
        this.objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
    }

    public String process(String record) throws IOException {
        LOG.debug("received debezium json data {} :", record);
        JsonNode recordRoot = objectMapper.readValue(record, JsonNode.class);
        String op = extractJsonNode(recordRoot, "op");
        if (Objects.isNull(op)) {
            // schema change ddl
            // starrocks 存算分离版本目前不支持schemaChange, 先注释掉
            // schemaChange(recordRoot);
            return INVALID_RESULT;
        }
        Map<String, String> valueMap;
        switch (op) {
            case OP_READ:
            case OP_CREATE:
            case OP_UPDATE:
                valueMap = extractAfterRow(recordRoot);
                addDeleteSign(valueMap, false);
                break;
            case OP_DELETE:
                valueMap = extractBeforeRow(recordRoot);
                addDeleteSign(valueMap, true);
                break;
            default:
                LOG.error("parse record fail, unknown op {} in {}", op, record);
                return INVALID_RESULT;
        }

        String format = objectMapper.writeValueAsString(valueMap);
        LOG.debug("format json data {} :", format);
        return format;
    }

    @VisibleForTesting
    public boolean schemaChange(JsonNode recordRoot) {
        boolean status = false;
        try{
            if(!StringUtils.isNullOrWhitespaceOnly(sourceTableName) && !checkTable(recordRoot)){
                return false;
            }
            String ddl = extractDDL(recordRoot);
            if(StringUtils.isNullOrWhitespaceOnly(ddl)){
                LOG.info("ddl can not do schema change:{}", recordRoot);
                return false;
            }
            // TODO Exec schema change
            LOG.info("schema change status:{}", status);
        }catch (Exception ex){
            LOG.warn("schema change error :", ex);
        }
        return status;
    }

    /**
     * When cdc synchronizes multiple tables, it will capture multiple table schema changes
     */
    protected boolean checkTable(JsonNode recordRoot) {
        String db = extractDatabase(recordRoot);
        String tbl = extractTable(recordRoot);
        String dbTbl = db + "." + tbl;
        return sourceTableName.equals(dbTbl);
    }

    private void addDeleteSign(Map<String, String> valueMap, boolean delete) {
        if(delete){
            valueMap.put(STARROCKS_DELETE_SIGN, "1");
        }else{
            valueMap.put(STARROCKS_DELETE_SIGN, "0");
        }
    }

    protected String extractDatabase(JsonNode record) {
        if(record.get("source").has("schema")){
            //compatible with schema
            return extractJsonNode(record.get("source"), "schema");
        }else{
            return extractJsonNode(record.get("source"), "db");
        }
    }

    protected String extractTable(JsonNode record) {
        return extractJsonNode(record.get("source"), "table");
    }

    private String extractJsonNode(JsonNode record, String key) {
        return record != null && record.get(key) != null ? record.get(key).asText() : null;
    }

    private Map<String, String> extractBeforeRow(JsonNode record) {
        return extractRow(record.get("before"));
    }

    private Map<String, String> extractAfterRow(JsonNode record) {
        return extractRow(record.get("after"));
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        Map<String, String> recordMap = objectMapper.convertValue(recordRow, new TypeReference<Map<String, String>>() {
        });
        return recordMap != null ? recordMap : new HashMap<>();
    }

    public String extractDDL(JsonNode record) throws JsonProcessingException {
        String historyRecord = extractJsonNode(record, "historyRecord");
        if (Objects.isNull(historyRecord)) {
            return null;
        }
        String ddl = extractJsonNode(objectMapper.readTree(historyRecord), "ddl");
        LOG.debug("received debezium ddl :{}", ddl);
        if (!Objects.isNull(ddl)) {
            //filter add/drop operation
            Matcher matcher = addDropDDLPattern.matcher(ddl);
            if(matcher.find()){
                String op = matcher.group(1);
                String col = matcher.group(3);
                String type = matcher.group(5);
                type = handleType(type);
                ddl = String.format(EXECUTE_DDL, starRocksOptions.getTableIdentifier(), op, col, type);
                LOG.info("parse ddl:{}", ddl);
                return ddl;
            }
        }
        return null;
    }

    public static DebeziumJsonSerializer.Builder builder() {
        return new DebeziumJsonSerializer.Builder();
    }

    /**
     * Builder for JsonDebeziumSchemaSerializer.
     */
    public static class Builder {
        private StarRocksOptions starRocksOptions;
        private Pattern addDropDDLPattern;
        private String sourceTableName;

        public DebeziumJsonSerializer.Builder setStarRocksOptions(StarRocksOptions starRocksOptions) {
            this.starRocksOptions = starRocksOptions;
            return this;
        }

        public DebeziumJsonSerializer.Builder setPattern(Pattern addDropDDLPattern) {
            this.addDropDDLPattern = addDropDDLPattern;
            return this;
        }

        public DebeziumJsonSerializer.Builder setSourceTableName(String sourceTableName) {
            this.sourceTableName = sourceTableName;
            return this;
        }

        public DebeziumJsonSerializer build() {
            return new DebeziumJsonSerializer(starRocksOptions, addDropDDLPattern, sourceTableName);
        }
    }

    private String handleType(String type) {

        if (type == null || "".equals(type)) {
            return "";
        }

        // varchar len * 3
        Pattern pattern = Pattern.compile("varchar\\(([1-9][0-9]*)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(type);
        if (matcher.find()) {
            String len = matcher.group(1);
            return String.format("varchar(%d)", Math.min(Integer.parseInt(len) * 3, 65533));
        }

        return type;

    }

}
