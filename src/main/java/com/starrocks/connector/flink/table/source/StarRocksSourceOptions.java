package com.starrocks.connector.flink.table.source;

import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

public class StarRocksSourceOptions implements Serializable {


    private final ReadableConfig tableOptions;
    private final Map<String, String> tableOptionsMap;
    private final Map<String, String> tableSQLProps = new HashMap<>();


    // required Options
    public static final ConfigOption<String> SCAN_URL = ConfigOptions.key("scan-url")
            .stringType().noDefaultValue().withDescription("Hosts of the fe node like: `fe_ip1:http_port,fe_ip2:http_port...`.");

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
    
    
    // optional Options
    public static final ConfigOption<Integer> SCAN_CONNECT_TIMEOUT = ConfigOptions.key("scan.connect.timeout-ms")
            .intType().defaultValue(1000).withDescription("Connect timeout");
        
    public static final ConfigOption<Integer> SCAN_BATCH_ROWS = ConfigOptions.key("scan.params.batch-rows")
            .intType().defaultValue(1000).withDescription("Batch rows");

    public static final ConfigOption<String> SCAN_PROPERTIES = ConfigOptions.key("scan.params.properties")
            .stringType().noDefaultValue().withDescription("Reserved params for use");
    
    public static final ConfigOption<Integer> SCAN_LIMIT = ConfigOptions.key("scan.params.limit")
            .intType().defaultValue(1).withDescription("The query limit, if specified.");

    public static final ConfigOption<Integer> SCAN_KEEP_ALIVE_MIN = ConfigOptions.key("scan.params.keep-alive-min")
            .intType().defaultValue(1).withDescription("Max keep alive time min");
    
    public static final ConfigOption<Integer> SCAN_QUERTY_TIMEOUT = ConfigOptions.key("scan.params.query-timeout")
            .intType().defaultValue(100).withDescription("Query timeout for a single query");

    public static final ConfigOption<Integer> SCAN_MEM_LIMIT = ConfigOptions.key("scan.params.mem-limit-byte")
            .intType().defaultValue(1024*1024*1024).withDescription("Memory limit for a single query");

    public static final ConfigOption<Integer> SCAN_MAX_RETRIES = ConfigOptions.key("scan.max-retries")
            .intType().defaultValue(1).withDescription("Max request retry times.");

    public static final ConfigOption<String> SCAN_COLUMNS = ConfigOptions.key("scan.columns")
            .stringType().defaultValue("").withDescription("SQL columns");

    public static final ConfigOption<String> SCAN_FILTER = ConfigOptions.key("scan.filter")
            .stringType().defaultValue("").withDescription("SQL filter");

    
    
    public static final String SOURCE_PROPERTIES_PREFIX = "scan.params.";

    public StarRocksSourceOptions(ReadableConfig options, Map<String, String> optionsMap) {
        this.tableOptions = options;
        this.tableOptionsMap = optionsMap;
        parseSourceProperties();
        this.validateRequired();
    }

    private void parseSourceProperties() {
        tableOptionsMap.keySet().stream()
                .filter(key -> key.startsWith(SOURCE_PROPERTIES_PREFIX))
                .forEach(key -> {
                    final String value = tableOptionsMap.get(key);
                    final String subKey = key.substring((SOURCE_PROPERTIES_PREFIX).length()).toLowerCase();
                    tableSQLProps.put(subKey, value);
                });
    }

    private void validateRequired() {
        ConfigOption<?>[] configOptions = new ConfigOption[]{
                USERNAME,
                PASSWORD,
                TABLE_NAME,
                DATABASE_NAME,
                SCAN_URL,
                JDBC_URL,
        };
        int presentCount = 0;
        for (ConfigOption<?> configOption : configOptions) {
            if (tableOptions.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n" + String.join("\n", propertyNames));
    }

    // required Options
    public String getScanUrl() {
        return tableOptions.get(SCAN_URL);
    }

    public String getJdbcUrl() {
        return tableOptions.get(JDBC_URL);
    }
    
    public String getUsername() {
        return tableOptions.get(USERNAME);
    }

    public String getPassword() {
        return tableOptions.get(PASSWORD);
    }

    public String getDatabaseName() {
        return tableOptions.get(DATABASE_NAME);
    }

    public String getTableName() {
        return tableOptions.get(TABLE_NAME);
    }


    // optional Options
    public int getConnectTimeoutMs() { 
        return tableOptions.get(SCAN_CONNECT_TIMEOUT).intValue(); 
    }

    public int getBatchRows() {
        return tableOptions.get(SCAN_BATCH_ROWS).intValue();
    }

    public Map<String, String> getProperties() {

        String json = tableOptions.get(SCAN_PROPERTIES);
        if (json == null) {
            return null;
        }
        Map<String, Object> stringToMap =  JSONObject.parseObject(json);
        Map<String, String> newMap = new HashMap<String,String>();
        for (Map.Entry<String, Object> entry : stringToMap.entrySet()) {
            if (entry.getValue() instanceof String) {
                newMap.put(entry.getKey(), (String) entry.getValue());
            }
        }
        return (Map<String, String>) newMap;
    }

    // public int getLimit() {
    //     return tableOptions.get(SCAN_LIMIT).intValue();
    // }

    public int getKeepAliveMin() {
        return tableOptions.get(SCAN_KEEP_ALIVE_MIN).intValue();
    }

    public int getQueryTimeout() {
        return tableOptions.get(SCAN_QUERTY_TIMEOUT).intValue();
    }

    public int getMemLimit() {
        return tableOptions.get(SCAN_MEM_LIMIT).intValue();
    }

    public int getScanMaxRetries() {
        return tableOptions.get(SCAN_MAX_RETRIES).intValue();
    }

    public String getColumns() {
        return tableOptions.get(SCAN_COLUMNS);
    }

    public String getFilter() {
        return tableOptions.get(SCAN_FILTER);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link StarRocksSourceOptions}.
     */
    public static final class Builder {
        private final Configuration conf;
        public Builder() {
            conf = new Configuration();
        }

        public StarRocksSourceOptions.Builder withProperty(String key, String value) {
            conf.setString(key, value);
            return this;
        }

        public StarRocksSourceOptions build() {
            return new StarRocksSourceOptions(conf, conf.toMap());
        }
    }
}