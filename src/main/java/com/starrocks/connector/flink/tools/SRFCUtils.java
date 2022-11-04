package com.starrocks.connector.flink.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class SRFCUtils {

    private static class SRFCPropertiesHolder {

        private static final Logger logger = LoggerFactory.getLogger(SRFCPropertiesHolder.class);
        private static final Properties instance = loadProperties();
        public static Properties loadProperties() {
            Properties props = new Properties();
            try {
                props.load(SRFCPropertiesHolder.class.getClassLoader().getResourceAsStream("srfc.properties"));
            } catch (IOException e) {
                logger.warn("load srfc properties failed.", e);
            }
            return props;
        }
    }

    public static String getSRFCVersion() {
        return SRFCPropertiesHolder.instance.getProperty("srfc.version", "");
    }
}
