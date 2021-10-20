/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.starrocks.connector.flink.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum TTableType implements org.apache.thrift.TEnum {
  MYSQL_TABLE(0),
  OLAP_TABLE(1),
  SCHEMA_TABLE(2),
  KUDU_TABLE(3),
  BROKER_TABLE(4),
  ES_TABLE(5),
  HDFS_TABLE(6),
  VIEW(20);

  private final int value;

  private TTableType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static TTableType findByValue(int value) { 
    switch (value) {
      case 0:
        return MYSQL_TABLE;
      case 1:
        return OLAP_TABLE;
      case 2:
        return SCHEMA_TABLE;
      case 3:
        return KUDU_TABLE;
      case 4:
        return BROKER_TABLE;
      case 5:
        return ES_TABLE;
      case 6:
        return HDFS_TABLE;
      case 20:
        return VIEW;
      default:
        return null;
    }
  }
}
