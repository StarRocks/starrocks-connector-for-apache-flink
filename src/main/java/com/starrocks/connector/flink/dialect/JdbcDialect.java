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
import com.starrocks.connector.flink.converter.JdbcRowConverter;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 *
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
     * Check if this dialect instance support a specific data type in table schema.
     *
     * @param rowType the physical table datatype of a row in the database table.
     * @exception ValidationException in case of the table schema contains unsupported type.
     */
    void validate(RowType rowType) throws ValidationException;


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



}
