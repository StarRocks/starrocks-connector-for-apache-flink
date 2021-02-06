package com.dorisdb.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Simple JDBC connection provider.
 */
public class DorisJdbcConnectionProvider implements DorisJdbcConnectionIProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final DorisJdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public DorisJdbcConnectionProvider(DorisJdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    Class.forName(jdbcOptions.getDriverName());
                    if (jdbcOptions.getUsername().isPresent()) {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL(), jdbcOptions.getUsername().get(), jdbcOptions.getPassword().orElse(null));
                    } else {
                        connection = DriverManager.getConnection(jdbcOptions.getDbURL());
                    }
                }
            }
        }
        return connection;
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.info("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
        connection = getConnection();
        return connection;
    }
}
