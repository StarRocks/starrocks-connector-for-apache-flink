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

public enum TStorageMedium implements org.apache.thrift.TEnum {
  HDD(0),
  SSD(1);

  private final int value;

  private TStorageMedium(int value) {
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
  public static TStorageMedium findByValue(int value) { 
    switch (value) {
      case 0:
        return HDD;
      case 1:
        return SSD;
      default:
        return null;
    }
  }
}