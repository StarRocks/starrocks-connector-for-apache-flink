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

public enum TCompressionType implements org.apache.thrift.TEnum {
  UNKNOWN_COMPRESSION(0),
  DEFAULT_COMPRESSION(1),
  NO_COMPRESSION(2),
  SNAPPY(3),
  LZ4(4),
  LZ4_FRAME(5),
  ZLIB(6),
  ZSTD(7),
  GZIP(8),
  DEFLATE(9),
  BZIP2(10),
  LZO(11);

  private final int value;

  private TCompressionType(int value) {
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
  public static TCompressionType findByValue(int value) { 
    switch (value) {
      case 0:
        return UNKNOWN_COMPRESSION;
      case 1:
        return DEFAULT_COMPRESSION;
      case 2:
        return NO_COMPRESSION;
      case 3:
        return SNAPPY;
      case 4:
        return LZ4;
      case 5:
        return LZ4_FRAME;
      case 6:
        return ZLIB;
      case 7:
        return ZSTD;
      case 8:
        return GZIP;
      case 9:
        return DEFLATE;
      case 10:
        return BZIP2;
      case 11:
        return LZO;
      default:
        return null;
    }
  }
}
