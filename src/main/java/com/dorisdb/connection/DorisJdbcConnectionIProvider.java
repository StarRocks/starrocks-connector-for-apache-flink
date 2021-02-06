package com.dorisdb.connection;

import org.apache.flink.annotation.Internal;
import java.sql.Connection;

/**
 * connection provider.
 */
@Internal
public interface DorisJdbcConnectionIProvider {
    Connection getConnection() throws Exception;

    Connection reestablishConnection() throws Exception;
}
