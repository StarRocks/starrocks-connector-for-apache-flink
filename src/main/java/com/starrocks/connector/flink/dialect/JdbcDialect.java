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

package com.starrocks.connector.flink.dialect;

import java.io.Serializable;
import java.util.Optional;
import com.starrocks.connector.flink.converter.JdbcRowConverter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 *
 * @see JdbcDialectFactory
 */
@PublicEvolving
public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Get converter that convert jdbc object and Flink internal object each other.
     *
     * @param rowType the given row type
     * @return a row converter for the database
     */
    JdbcRowConverter getRowConverter(RowType rowType);

    /**
     * Get limit clause to limit the number of emitted row from the jdbc source.
     *
     * @param limit number of row to emit. The value of the parameter should be non-negative.
     * @return the limit clause.
     */
    String getLimitClause(long limit);

    /**
     * Check if this dialect instance support a specific data type in table schema.
     *
     * @param rowType the physical table datatype of a row in the database table.
     * @exception ValidationException in case of the table schema contains unsupported type.
     */
    void validate(RowType rowType) throws ValidationException;

    /**
     * @return the default driver class name, if user has not configured the driver class name, then
     *     this one will be used.
     */
    default Optional<String> defaultDriverName() {
        return Optional.empty();
    }

    /**
     * Quotes the identifier.
     *
     * <p>Used to put quotes around the identifier if the column name is a reserved keyword or
     * contains characters requiring quotes (e.g., space).
     * @param identifier identifier
     *
     * @return the quoted identifier.
     */
    String quoteIdentifier(String identifier);



    /**
     * Constructs the dialects select statement for fields with given conditions. The returned
     * string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement must be
     * in the same order as the {@code fieldNames} parameter.
     *
     * @return A select statement.
     */
    String getSelectFromStatement(
            String tableName, String[] selectFields, String[] conditionFields);

    /**
     * Appends default JDBC properties to url for current dialect. Some database dialects will set
     * default JDBC properties for performance or optimization consideration, such as MySQL dialect
     * uses 'rewriteBatchedStatements=true' to enable execute multiple MySQL statements in batch
     * mode.
     *
     * @return A JDBC url that has appended the default properties.
     */
    default String appendDefaultUrlProperties(String url) {
        return url;
    }
}
