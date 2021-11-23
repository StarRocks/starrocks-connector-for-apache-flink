package com.starrocks.connector.flink.table.source.struct;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Const {
    public static String DEFAULT_CLUSTER_NAME = "default_cluster";

    // Flink
    public static final String DATA_TYPE_FLINK_DATE = "DATE";
    public static final String DATA_TYPE_FLINK_TIMESTAMP = "TIMESTAMP";

    public static final String DATA_TYPE_FLINK_STRING = "STRING";
    public static final String DATA_TYPE_FLINK_CHAR = "CHAR";

    public static final String DATA_TYPE_FLINK_BOOLEAN = "BOOLEAN";

    public static final String DATA_TYPE_FLINK_TINYINT = "TINYINT";
    public static final String DATA_TYPE_FLINK_SMALLINT = "SMALLINT";
    public static final String DATA_TYPE_FLINK_INT = "INT";
    public static final String DATA_TYPE_FLINK_BIGINT = "BIGINT";

    public static final String DATA_TYPE_FLINK_FLOAT = "FLOAT";
    public static final String DATA_TYPE_FLINK_DOUBLE = "DOUBLE";
    public static final String DATA_TYPE_FLINK_DECIMAL = "DECIMAL";

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
    public static final String DATA_TYPE_STARROCKS_DECIMAL128 = "DECIMAL128";

    public static HashMap<String, Set<String>> DataTypeRelationMap = new HashMap<String, Set<String>>() {{
            put(DATA_TYPE_FLINK_DATE, new HashSet<String>(){{
                    add(DATA_TYPE_STARROCKS_DATE);
                }
            });
            put(DATA_TYPE_FLINK_TIMESTAMP, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DATETIME);
                }
            });
            put(DATA_TYPE_FLINK_STRING, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_CHAR);
                add(DATA_TYPE_STARROCKS_VARCHAR);
                add(DATA_TYPE_STARROCKS_LARGEINT);
                }
            });
            put(DATA_TYPE_FLINK_CHAR, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_CHAR);
                }
            });
            put(DATA_TYPE_FLINK_BOOLEAN, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_BOOLEAN);
                }
            });
            put(DATA_TYPE_FLINK_TINYINT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_TINYINT);
                }
            });
            put(DATA_TYPE_FLINK_SMALLINT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_SMALLINT);
                }
            });
            put(DATA_TYPE_FLINK_INT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_INT);
                }
            });
            put(DATA_TYPE_FLINK_BIGINT, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_BIGINT);
                }
            });
            put(DATA_TYPE_FLINK_FLOAT, new HashSet<String>(){{
                add(DATA_TYPE_FLINK_FLOAT);
                }
            });
            put(DATA_TYPE_FLINK_DOUBLE, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DOUBLE);
                }
            });
            put(DATA_TYPE_FLINK_DECIMAL, new HashSet<String>(){{
                add(DATA_TYPE_STARROCKS_DECIMAL);
                add(DATA_TYPE_STARROCKS_DECIMALV2);
                add(DATA_TYPE_STARROCKS_DECIMAL128);
                }
            });
        }
    };
}
