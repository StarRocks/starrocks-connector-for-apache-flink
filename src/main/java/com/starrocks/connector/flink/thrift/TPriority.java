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

public enum TPriority implements org.apache.thrift.TEnum {
  NORMAL(0),
  HIGH(1);

  private final int value;

  private TPriority(int value) {
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
  public static TPriority findByValue(int value) { 
    switch (value) {
      case 0:
        return NORMAL;
      case 1:
        return HIGH;
      default:
        return null;
    }
  }
}
