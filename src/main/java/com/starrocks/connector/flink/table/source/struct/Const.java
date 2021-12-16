package com.starrocks.connector.flink.table.source.struct;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

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
    

    public static HashMap<LogicalTypeRoot, Set<String>> DataTypeRelationMap = new HashMap<LogicalTypeRoot, Set<String>>() {{
            put(LogicalTypeRoot.DATE, new HashSet<String>(){{
                    add(DATA_TYPE_STARROCKS_DATE);
                }
            });
            put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DATETIME);
                }
            });
            put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DATETIME);
                }
            });
            put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DATETIME);
                }
            });
            put(LogicalTypeRoot.CHAR, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_CHAR);
                }
            });
            put(LogicalTypeRoot.VARCHAR, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_VARCHAR);
                add(DATA_TYPE_STARROCKS_LARGEINT);
                }
            });
            put(LogicalTypeRoot.BOOLEAN, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_BOOLEAN);
                }
            });
            put(LogicalTypeRoot.TINYINT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_TINYINT);
                }
            });
            put(LogicalTypeRoot.SMALLINT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_SMALLINT);
                }
            });
            put(LogicalTypeRoot.INTEGER, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_INT);
                }
            });
            put(LogicalTypeRoot.BIGINT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_BIGINT);
                }
            });
            put(LogicalTypeRoot.FLOAT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_FLOAT);
                }
            });
            put(LogicalTypeRoot.DOUBLE, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DOUBLE);
                }
            });
            put(LogicalTypeRoot.DECIMAL, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DECIMAL);
                add(DATA_TYPE_STARROCKS_DECIMALV2);
                add(DATA_TYPE_STARROCKS_DECIMAL32);
                add(DATA_TYPE_STARROCKS_DECIMAL64);
                add(DATA_TYPE_STARROCKS_DECIMAL128);
                }
            });
        }
    };
}
