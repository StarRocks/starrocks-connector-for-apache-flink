package com.starrocks.data.load.stream;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class StreamLoadUtils {

    public static String getTableUniqueKey(String database, String table) {
        return database + "-" + table;
    }

    public static String getStreamLoadUrl(String host, String database, String table) {
        if (host == null) {
            throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
        }
        return host +
                "/api/" +
                database +
                "/" +
                table +
                "/_stream_load";
    }

    public static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }
}
