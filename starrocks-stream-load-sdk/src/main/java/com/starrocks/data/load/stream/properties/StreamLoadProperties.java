package com.starrocks.data.load.stream.properties;

import com.starrocks.data.load.stream.StreamLoadUtils;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StreamLoadProperties implements Serializable {
    private final String jdbcUrl;
    private final String[] loadUrls;
    private final String username;
    @JSONField(serialize = false)
    private final String password;
    private final String version;

    private final String labelPrefix;

    private final StreamLoadTableProperties defaultTableProperties;
    private final Map<String, StreamLoadTableProperties> tablePropertiesMap;

    private final boolean enableTransaction;

    // manager settings
    /**
     * ms
     * manager线程扫描频率
     */
    private final long scanningFrequency;
    /**
     * 最大缓存空间
     */
    private final long maxCacheBytes;
    /**
     * ms
     * 期望的单表延时时长
     */
    private final long expectDelayTime;

    // http client settings
    /**
     * ms
     */
    private final int connectTimeout;
    private final int socketTimeout;
    private final int ioThreadCount;

    // default strategy settings
    /**
     * ms
     * 多少时间范围内被视为一直写
     */
    private final long writingThreshold;
    /**
     * 当region占比高于多少时，触发flush
     */
    private final float regionBufferRatio;
    private final float youngThreshold;
    private final float oldThreshold;

    private final Map<String, String> headers;

    private StreamLoadProperties(Builder builder) {
        this.jdbcUrl = builder.jdbcUrl;
        this.loadUrls = builder.loadUrls;
        this.username = builder.username;
        this.password = builder.password;
        this.version = builder.version;

        this.enableTransaction = builder.enableTransaction;

        this.labelPrefix = builder.labelPrefix;

        this.defaultTableProperties = builder.defaultTableProperties;
        this.tablePropertiesMap = builder.tablePropertiesMap;

        this.scanningFrequency = builder.scanningFrequency;
        this.maxCacheBytes = builder.maxCacheBytes;
        this.expectDelayTime = builder.expectDelayTime;

        this.connectTimeout = builder.connectTimeout;
        this.socketTimeout = builder.socketTimeout;
        this.ioThreadCount = builder.ioThreadCount;

        this.writingThreshold = builder.writingThreshold;
        this.regionBufferRatio = builder.regionBufferRatio;
        this.youngThreshold = builder.youngThreshold;
        this.oldThreshold = builder.oldThreshold;

        this.headers = Collections.unmodifiableMap(builder.headers);
    }

    public boolean isEnableTransaction() {
        return enableTransaction;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String[] getLoadUrls() {
        return loadUrls;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getVersion() {
        return version;
    }

    public boolean isOpAutoProjectionInJson() {
        return version == null || version.length() > 0 && !version.trim().startsWith("1.");
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public StreamLoadTableProperties getDefaultTableProperties() {
        return defaultTableProperties;
    }

    public StreamLoadTableProperties getTableProperties(String database, String table) {
        return getTableProperties(StreamLoadUtils.getTableUniqueKey(database, table));
    }

    public StreamLoadTableProperties getTableProperties(String uniqueKey) {
        return tablePropertiesMap.getOrDefault(uniqueKey, defaultTableProperties);
    }

    public Map<String, StreamLoadTableProperties> getTablePropertiesMap() {
        return tablePropertiesMap;
    }

    public long getScanningFrequency() {
        return scanningFrequency;
    }

    public long getMaxCacheBytes() {
        return maxCacheBytes;
    }

    public long getExpectDelayTime() {
        return expectDelayTime;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getIoThreadCount() {
        return ioThreadCount;
    }

    public long getWritingThreshold() {
        return writingThreshold;
    }

    public float getRegionBufferRatio() {
        return regionBufferRatio;
    }

    public float getYoungThreshold() {
        return youngThreshold;
    }

    public float getOldThreshold() {
        return oldThreshold;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String jdbcUrl;
        private String[] loadUrls;
        private String username;
        private String password;
        private String version;

        private boolean enableTransaction;

        private String labelPrefix = "";

        private long scanningFrequency = 50L;
        private long maxCacheBytes = (long) (Runtime.getRuntime().freeMemory() * 0.7);
        private long expectDelayTime = 300000L;

        private StreamLoadTableProperties defaultTableProperties;
        private Map<String, StreamLoadTableProperties> tablePropertiesMap = new HashMap<>();

        private int connectTimeout = 60000;
        private int socketTimeout;
        private int ioThreadCount = Runtime.getRuntime().availableProcessors();

        private long writingThreshold = 50L;
        private float regionBufferRatio = 0.6F;
        private float youngThreshold = 0.1F;
        private float oldThreshold = 0.9F;

        private Map<String, String> headers = new HashMap<>();

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder loadUrls(String... loadUrls) {
            this.loadUrls = loadUrls;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder enableTransaction() {
            this.enableTransaction = true;
            return this;
        }

        public Builder labelPrefix(String labelPrefix) {
            this.labelPrefix = labelPrefix;
            return this;
        }

        public Builder defaultTableProperties(StreamLoadTableProperties tableProperties) {
            this.defaultTableProperties = tableProperties;
            return this;
        }

        public Builder addTableProperties(StreamLoadTableProperties tableProperties) {
            if (defaultTableProperties == null) {
                defaultTableProperties = tableProperties;
            }
            tablePropertiesMap.put(tableProperties.getUniqueKey(), tableProperties);
            return this;
        }

        public Builder scanningFrequency(long scanningFrequency) {
            if (scanningFrequency < 50) {
                throw new IllegalArgumentException("scanningFrequency `" + scanningFrequency + "ms` set failed, must greater or equals to 50");
            }
            this.scanningFrequency = scanningFrequency;
            return this;
        }

        public Builder cacheMaxBytes(long maxCacheBytes) {
            if (maxCacheBytes <= 0) {
                throw new IllegalArgumentException("cacheMaxBytes `" + maxCacheBytes + "` set failed, must greater to 0");
            }
            if (maxCacheBytes > Runtime.getRuntime().maxMemory()) {
                throw new IllegalArgumentException("cacheMaxBytes `" + maxCacheBytes + "` set failed, current maxMemory is " + Runtime.getRuntime().maxMemory());
            }
            this.maxCacheBytes = maxCacheBytes;
            return this;
        }

        public Builder expectDelayTime(long expectDelayTime) {
            if (expectDelayTime <= 0) {
                throw new IllegalArgumentException("expectDelayTime `" + expectDelayTime + "ms` set failed, must greater to 0");
            }
            this.expectDelayTime = expectDelayTime;
            return this;
        }

        public Builder connectTimeout(int connectTimeout) {
            if (connectTimeout < 100 || connectTimeout > 60000) {
                throw new IllegalArgumentException("connectTimeout `" + connectTimeout + "ms` set failed, must range in [100, 60000]");
            }
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder socketTimeout(int socketTimeout) {
            if (socketTimeout < 0) {
                throw new IllegalArgumentException("socketTimeout `" + socketTimeout + "ms` set failed, must greater or equals to 0");
            }
            this.socketTimeout = socketTimeout;
            return this;
        }

        public Builder ioThreadCount(int ioThreadCount) {
            if (ioThreadCount <= 0) {
                throw new IllegalArgumentException("ioThreadCount `" + ioThreadCount + "` set failed, must greater to 0");
            }
            this.ioThreadCount = ioThreadCount;
            return this;
        }

        public Builder writingThreshold(long writingThreshold) {
            this.writingThreshold = writingThreshold;
            return this;
        }

        public Builder regionBufferRatio(float regionBufferRatio) {
            if (regionBufferRatio <= 0 || regionBufferRatio > 1) {
                throw new IllegalArgumentException("regionBufferRatio `" + regionBufferRatio + "` set failed, must range in (0, 1]");
            }
            this.regionBufferRatio = regionBufferRatio;
            return this;
        }

        public Builder youngThreshold(float youngThreshold) {
            if (youngThreshold <= 0 || youngThreshold > 1) {
                throw new IllegalArgumentException("youngThreshold `" + youngThreshold + "` set failed, must range in (0, 1]");
            }
            this.youngThreshold = youngThreshold;
            return this;
        }

        public Builder oldThreshold(float oldThreshold) {
            if (oldThreshold <= 0 || oldThreshold > 1) {
                throw new IllegalArgumentException("youngThreshold `" + oldThreshold + "` set failed, must range in (0, 1]");
            }
            this.oldThreshold = oldThreshold;
            return this;
        }

        public Builder addHeader(String name, String value) {
            headers.put(name, value);
            return this;
        }

        public Builder addHeaders(Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public StreamLoadProperties build() {
            StreamLoadProperties streamLoadProperties = new StreamLoadProperties(this);

            if (streamLoadProperties.getYoungThreshold() >= streamLoadProperties.getOldThreshold()) {
                throw new IllegalArgumentException(String.format("oldThreshold(`%s`) must greater to youngThreshold(`%s`)",
                        streamLoadProperties.getOldThreshold(), streamLoadProperties.getYoungThreshold()));
            }

            if (streamLoadProperties.getExpectDelayTime() < streamLoadProperties.getScanningFrequency()) {
                throw new IllegalArgumentException(String.format("expectDelayTime(`%s`) must greater to scanningFrequency(`%s`)",
                        streamLoadProperties.getExpectDelayTime(), streamLoadProperties.getScanningFrequency()));
            }
            return streamLoadProperties;
        }

    }
}
