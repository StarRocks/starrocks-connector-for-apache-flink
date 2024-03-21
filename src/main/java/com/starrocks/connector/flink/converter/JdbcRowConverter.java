/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.converter;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.starrocks.connector.flink.statement.FieldNamedPreparedStatement;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.RowData;

/**
 * Converter that is responsible to convert between JDBC object and Flink SQL internal data
 * structure {@link RowData}.
 */
@PublicEvolving
public interface JdbcRowConverter extends Serializable {

    /**
     * Convert data retrieved from {@link ResultSet} to internal {@link RowData}.
     *
     * @param resultSet ResultSet from JDBC
     * @return resultSet to row
     * @throws SQLException sql exception
     */
    RowData toInternal(ResultSet resultSet) throws SQLException;

    /**
     * Convert data retrieved from Flink internal RowData to JDBC Object.
     *
     * @param rowData The given internal {@link RowData}.
     * @param statement The statement to be filled.
     * @return The filled statement.
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     * if a database access error occurs or this method is called on a closed PreparedStatement
     */
    FieldNamedPreparedStatement toExternal(RowData rowData, FieldNamedPreparedStatement statement)
            throws SQLException;
}
