package com.starrocks.data.load.stream;

import com.alibaba.fastjson.JSON;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public interface StreamLoadStrategy extends Serializable {

    List<TableRegion> select(Iterable<TableRegion> regions);

    class DefaultLoadStrategy implements StreamLoadStrategy {

        private static final long serialVersionUID = 1L;

        private static final Logger log = LoggerFactory.getLogger(DefaultLoadStrategy.class);

        private final long oldAge;
        private final long youngAge;
        private final long cacheMaxBytes;
        private final long writingThreshold;
        private final float regionBufferRatio;
        private final float cacheLimitBytes;

        public DefaultLoadStrategy(StreamLoadProperties properties) {
            long expectDelayTime = properties.getExpectDelayTime();
            this.youngAge = (long) ((expectDelayTime / properties.getScanningFrequency()) * properties.getYoungThreshold());
            this.oldAge = (long) ((expectDelayTime / properties.getScanningFrequency()) * properties.getOldThreshold());
            this.cacheMaxBytes = properties.getMaxCacheBytes();
            this.cacheLimitBytes = cacheMaxBytes * 0.8F;
            this.writingThreshold = properties.getWritingThreshold();
            this.regionBufferRatio = properties.getRegionBufferRatio();
            log.info("Load Strategy properties : {}", JSON.toJSONString(this));
        }

        @Override
        public List<TableRegion> select(Iterable<TableRegion> regions) {
            List<TableRegion> waitFlushRegions = new ArrayList<>();
            List<TableRegion> youngRegions = new ArrayList<>();

            int totalReadableRegion = 0;
            for (TableRegion region : regions) {
                long age = region.getAndIncrementAge();
                totalReadableRegion++;
                if (age >= oldAge) {
                    waitFlushRegions.add(region);
                } else if (age >= youngAge) {
                    youngRegions.add(region);
                }
            }
            if (totalReadableRegion == 0) {
                return waitFlushRegions;
            }

            long cacheThreshold = (long) ((cacheMaxBytes / totalReadableRegion) * regionBufferRatio);
            long currentTimeMillis = System.currentTimeMillis();
            for (TableRegion region : youngRegions) {
                if (region.getCacheBytes() < cacheThreshold) {
                    continue;
                }
                if (region.getCacheBytes() < cacheLimitBytes
                        && currentTimeMillis - region.getLastWriteTimeMillis() < writingThreshold) {
                    continue;
                }
                waitFlushRegions.add(region);
            }

            waitFlushRegions.sort(Comparator.comparingLong(TableRegion::getCacheBytes).reversed());
            return waitFlushRegions;
        }

        public long getOldAge() {
            return oldAge;
        }

        public long getYoungAge() {
            return youngAge;
        }

        public long getCacheMaxBytes() {
            return cacheMaxBytes;
        }

        public long getWritingThreshold() {
            return writingThreshold;
        }

        public float getRegionBufferRatio() {
            return regionBufferRatio;
        }
    }
}
