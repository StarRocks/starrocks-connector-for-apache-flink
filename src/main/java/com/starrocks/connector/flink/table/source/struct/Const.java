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

package com.starrocks.connector.flink.table.source.struct;

import java.util.HashMap;


import com.starrocks.connector.flink.row.source.StarRocksToFlinkTrans;
import com.starrocks.connector.flink.row.source.StarRocksToFlinkTranslators;

import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class Const {

    public static String DEFAULT_CLUSTER_NAME = "default_cluster";

    // StarRocks
    public static final String DATA_TYPE_STARROCKS_DATE = "DATE";
    public static final String DATA_TYPE_STARROCKS_DATETIME = "DATETIME";

    public static final String DATA_TYPE_STARROCKS_CHAR = "CHAR";
    public static final String DATA_TYPE_STARROCKS_VARCHAR = "VARCHAR";

    public static final String DATA_TYPE_STARROCKS_BOOLEAN = "BOOLEAN";

    public static final String DATA_TYPE_STARROCKS_TINYINT = "TINYINT";
    public static final String DATA_TYPE_STARROCKS_SMALLINT = "SMALLINT";
    public static final String DATA_TYPE_STARROCKS_INT = "INT";
    public static final String DATA_TYPE_STARROCKS_BIGINT = "BIGINT";
    public static final String DATA_TYPE_STARROCKS_LARGEINT = "LARGEINT";

    public static final String DATA_TYPE_STARROCKS_FLOAT = "FLOAT";
    public static final String DATA_TYPE_STARROCKS_DOUBLE = "DOUBLE";
    public static final String DATA_TYPE_STARROCKS_DECIMAL = "DECIMAL";
    public static final String DATA_TYPE_STARROCKS_DECIMALV2 = "DECIMALV2";
    public static final String DATA_TYPE_STARROCKS_DECIMAL32 = "DECIMAL32";
    public static final String DATA_TYPE_STARROCKS_DECIMAL64 = "DECIMAL64";
    public static final String DATA_TYPE_STARROCKS_DECIMAL128 = "DECIMAL128";
    

    public static HashMap<LogicalTypeRoot, HashMap<String, StarRocksToFlinkTrans>> DataTypeRelationMap = new HashMap<LogicalTypeRoot, HashMap<String, StarRocksToFlinkTrans>>();

    static {
        DataTypeRelationMap.put(LogicalTypeRoot.DATE, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_DATE, new StarRocksToFlinkTranslators().new ToFlinkDate());
            }
        });

        DataTypeRelationMap.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_DATETIME, new StarRocksToFlinkTranslators().new ToFlinkTimestamp());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_DATETIME, new StarRocksToFlinkTranslators().new ToFlinkTimestamp());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_DATETIME, new StarRocksToFlinkTranslators().new ToFlinkTimestamp());
            }
        });

        DataTypeRelationMap.put(LogicalTypeRoot.CHAR, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_CHAR, new StarRocksToFlinkTranslators().new ToFlinkChar());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.VARCHAR, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_VARCHAR, new StarRocksToFlinkTranslators().new ToFlinkChar());
                put(DATA_TYPE_STARROCKS_LARGEINT, new StarRocksToFlinkTranslators().new ToFlinkChar());
            }
        });

        DataTypeRelationMap.put(LogicalTypeRoot.BOOLEAN, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_BOOLEAN, new StarRocksToFlinkTranslators().new ToFlinkBoolean());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.TINYINT, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_TINYINT, new StarRocksToFlinkTranslators().new ToFlinkTinyInt());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.SMALLINT, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_SMALLINT, new StarRocksToFlinkTranslators().new ToFlinkSmallInt());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.INTEGER, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_INT, new StarRocksToFlinkTranslators().new ToFlinkInt());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.BIGINT, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_BIGINT, new StarRocksToFlinkTranslators().new ToFlinkBigInt());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.FLOAT, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_FLOAT, new StarRocksToFlinkTranslators().new ToFlinkFloat());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.DOUBLE, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_DOUBLE, new StarRocksToFlinkTranslators().new ToFlinkDouble());
            }
        });
        DataTypeRelationMap.put(LogicalTypeRoot.DECIMAL, new HashMap<String, StarRocksToFlinkTrans>() {{
                put(DATA_TYPE_STARROCKS_DECIMAL, new StarRocksToFlinkTranslators().new ToFlinkDecimal());
                put(DATA_TYPE_STARROCKS_DECIMALV2, new StarRocksToFlinkTranslators().new ToFlinkDecimal());
                put(DATA_TYPE_STARROCKS_DECIMAL32, new StarRocksToFlinkTranslators().new ToFlinkDecimal());
                put(DATA_TYPE_STARROCKS_DECIMAL64, new StarRocksToFlinkTranslators().new ToFlinkDecimal());
                put(DATA_TYPE_STARROCKS_DECIMAL128, new StarRocksToFlinkTranslators().new ToFlinkDecimal());
            }
        });
    }
}
