/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.starrocks.connector.flink.thrift;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class TNetworkAddress implements org.apache.thrift.TBase<TNetworkAddress, TNetworkAddress._Fields>, java.io.Serializable, Cloneable, Comparable<TNetworkAddress> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TNetworkAddress");

  private static final org.apache.thrift.protocol.TField HOSTNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("hostname", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField PORT_FIELD_DESC = new org.apache.thrift.protocol.TField("port", org.apache.thrift.protocol.TType.I32, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TNetworkAddressStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TNetworkAddressTupleSchemeFactory());
  }

  public String hostname; // required
  public int port; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    HOSTNAME((short)1, "hostname"),
    PORT((short)2, "port");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // HOSTNAME
          return HOSTNAME;
        case 2: // PORT
          return PORT;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __PORT_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.HOSTNAME, new org.apache.thrift.meta_data.FieldMetaData("hostname", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PORT, new org.apache.thrift.meta_data.FieldMetaData("port", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TNetworkAddress.class, metaDataMap);
  }

  public TNetworkAddress() {
  }

  public TNetworkAddress(
    String hostname,
    int port)
  {
    this();
    this.hostname = hostname;
    this.port = port;
    setPortIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TNetworkAddress(TNetworkAddress other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetHostname()) {
      this.hostname = other.hostname;
    }
    this.port = other.port;
  }

  public TNetworkAddress deepCopy() {
    return new TNetworkAddress(this);
  }

  @Override
  public void clear() {
    this.hostname = null;
    setPortIsSet(false);
    this.port = 0;
  }

  public String getHostname() {
    return this.hostname;
  }

  public TNetworkAddress setHostname(String hostname) {
    this.hostname = hostname;
    return this;
  }

  public void unsetHostname() {
    this.hostname = null;
  }

  /** Returns true if field hostname is set (has been assigned a value) and false otherwise */
  public boolean isSetHostname() {
    return this.hostname != null;
  }

  public void setHostnameIsSet(boolean value) {
    if (!value) {
      this.hostname = null;
    }
  }

  public int getPort() {
    return this.port;
  }

  public TNetworkAddress setPort(int port) {
    this.port = port;
    setPortIsSet(true);
    return this;
  }

  public void unsetPort() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __PORT_ISSET_ID);
  }

  /** Returns true if field port is set (has been assigned a value) and false otherwise */
  public boolean isSetPort() {
    return EncodingUtils.testBit(__isset_bitfield, __PORT_ISSET_ID);
  }

  public void setPortIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __PORT_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case HOSTNAME:
      if (value == null) {
        unsetHostname();
      } else {
        setHostname((String)value);
      }
      break;

    case PORT:
      if (value == null) {
        unsetPort();
      } else {
        setPort((Integer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case HOSTNAME:
      return getHostname();

    case PORT:
      return Integer.valueOf(getPort());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case HOSTNAME:
      return isSetHostname();
    case PORT:
      return isSetPort();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TNetworkAddress)
      return this.equals((TNetworkAddress)that);
    return false;
  }

  public boolean equals(TNetworkAddress that) {
    if (that == null)
      return false;

    boolean this_present_hostname = true && this.isSetHostname();
    boolean that_present_hostname = true && that.isSetHostname();
    if (this_present_hostname || that_present_hostname) {
      if (!(this_present_hostname && that_present_hostname))
        return false;
      if (!this.hostname.equals(that.hostname))
        return false;
    }

    boolean this_present_port = true;
    boolean that_present_port = true;
    if (this_present_port || that_present_port) {
      if (!(this_present_port && that_present_port))
        return false;
      if (this.port != that.port)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TNetworkAddress other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetHostname()).compareTo(other.isSetHostname());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHostname()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hostname, other.hostname);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPort()).compareTo(other.isSetPort());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPort()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.port, other.port);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TNetworkAddress(");
    boolean first = true;

    sb.append("hostname:");
    if (this.hostname == null) {
      sb.append("null");
    } else {
      sb.append(this.hostname);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("port:");
    sb.append(this.port);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (hostname == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'hostname' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'port' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TNetworkAddressStandardSchemeFactory implements SchemeFactory {
    public TNetworkAddressStandardScheme getScheme() {
      return new TNetworkAddressStandardScheme();
    }
  }

  private static class TNetworkAddressStandardScheme extends StandardScheme<TNetworkAddress> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TNetworkAddress struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // HOSTNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.hostname = iprot.readString();
              struct.setHostnameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PORT
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.port = iprot.readI32();
              struct.setPortIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetPort()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'port' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TNetworkAddress struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.hostname != null) {
        oprot.writeFieldBegin(HOSTNAME_FIELD_DESC);
        oprot.writeString(struct.hostname);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(PORT_FIELD_DESC);
      oprot.writeI32(struct.port);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TNetworkAddressTupleSchemeFactory implements SchemeFactory {
    public TNetworkAddressTupleScheme getScheme() {
      return new TNetworkAddressTupleScheme();
    }
  }

  private static class TNetworkAddressTupleScheme extends TupleScheme<TNetworkAddress> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TNetworkAddress struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.hostname);
      oprot.writeI32(struct.port);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TNetworkAddress struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.hostname = iprot.readString();
      struct.setHostnameIsSet(true);
      struct.port = iprot.readI32();
      struct.setPortIsSet(true);
    }
  }

}

