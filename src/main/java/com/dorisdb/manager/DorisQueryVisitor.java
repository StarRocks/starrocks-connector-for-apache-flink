package com.dorisdb.manager;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.sql.ResultSetMetaData;
import java.util.Map;

import com.dorisdb.connection.DorisJdbcConnectionProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class DorisQueryVisitor implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DorisQueryVisitor.class);

    private final DorisJdbcConnectionProvider jdbcConnProvider;
    private final String database;
    private final String table;

    public DorisQueryVisitor(DorisJdbcConnectionProvider jdbcConnProvider, String database, String table) {
        this.jdbcConnProvider = jdbcConnProvider;
        this.database = database;
        this.table = table;
    }

    public List<Map<String, Object>> getTableColumnsMetaData() {
        final String query = "select `COLUMN_NAME`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_SCHEMA`=?;";
        List<Map<String, Object>> rows;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing query '%s'", query));
            }
            rows = executQuery(query, this.database, this.table);
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get table schema info from doris." + se.getMessage(), se);
        }
        return rows;
    }

    private List<Map<String, Object>> executQuery(String query, String... args) throws ClassNotFoundException, SQLException {
        Connection dbConn = jdbcConnProvider.getConnection();
        PreparedStatement stmt = dbConn.prepareStatement(query);
        for (int i = 0; i < args.length; i++) {
            stmt.setString(i + 1, args[i]);
        }
        ResultSet rs = stmt.executeQuery();
        rs.next();
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();
        int currRowIndex = rs.getRow();
        rs.beforeFirst();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }
            list.add(row);
        }
        rs.absolute(currRowIndex);
        return list;
    }
}
