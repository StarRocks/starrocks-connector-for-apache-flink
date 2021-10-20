/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.starrocks.connector.flink.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TScanCloseParams implements org.apache.thrift.TBase<TScanCloseParams, TScanCloseParams._Fields>, java.io.Serializable, Cloneable, Comparable<TScanCloseParams> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TScanCloseParams");

  private static final org.apache.thrift.protocol.TField CONTEXT_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("context_id", org.apache.thrift.protocol.TType.STRING, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TScanCloseParamsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TScanCloseParamsTupleSchemeFactory());
  }

  public String context_id; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    CONTEXT_ID((short)1, "context_id");

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
        case 1: // CONTEXT_ID
          return CONTEXT_ID;
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
  private _Fields optionals[] = {_Fields.CONTEXT_ID};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.CONTEXT_ID, new org.apache.thrift.meta_data.FieldMetaData("context_id", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TScanCloseParams.class, metaDataMap);
  }

  public TScanCloseParams() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TScanCloseParams(TScanCloseParams other) {
    if (other.isSetContext_id()) {
      this.context_id = other.context_id;
    }
  }

  public TScanCloseParams deepCopy() {
    return new TScanCloseParams(this);
  }

  @Override
  public void clear() {
    this.context_id = null;
  }

  public String getContext_id() {
    return this.context_id;
  }

  public TScanCloseParams setContext_id(String context_id) {
    this.context_id = context_id;
    return this;
  }

  public void unsetContext_id() {
    this.context_id = null;
  }

  /** Returns true if field context_id is set (has been assigned a value) and false otherwise */
  public boolean isSetContext_id() {
    return this.context_id != null;
  }

  public void setContext_idIsSet(boolean value) {
    if (!value) {
      this.context_id = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case CONTEXT_ID:
      if (value == null) {
        unsetContext_id();
      } else {
        setContext_id((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case CONTEXT_ID:
      return getContext_id();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case CONTEXT_ID:
      return isSetContext_id();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TScanCloseParams)
      return this.equals((TScanCloseParams)that);
    return false;
  }

  public boolean equals(TScanCloseParams that) {
    if (that == null)
      return false;

    boolean this_present_context_id = true && this.isSetContext_id();
    boolean that_present_context_id = true && that.isSetContext_id();
    if (this_present_context_id || that_present_context_id) {
      if (!(this_present_context_id && that_present_context_id))
        return false;
      if (!this.context_id.equals(that.context_id))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TScanCloseParams other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetContext_id()).compareTo(other.isSetContext_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetContext_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.context_id, other.context_id);
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
    StringBuilder sb = new StringBuilder("TScanCloseParams(");
    boolean first = true;

    if (isSetContext_id()) {
      sb.append("context_id:");
      if (this.context_id == null) {
        sb.append("null");
      } else {
        sb.append(this.context_id);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TScanCloseParamsStandardSchemeFactory implements SchemeFactory {
    public TScanCloseParamsStandardScheme getScheme() {
      return new TScanCloseParamsStandardScheme();
    }
  }

  private static class TScanCloseParamsStandardScheme extends StandardScheme<TScanCloseParams> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TScanCloseParams struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CONTEXT_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.context_id = iprot.readString();
              struct.setContext_idIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TScanCloseParams struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.context_id != null) {
        if (struct.isSetContext_id()) {
          oprot.writeFieldBegin(CONTEXT_ID_FIELD_DESC);
          oprot.writeString(struct.context_id);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TScanCloseParamsTupleSchemeFactory implements SchemeFactory {
    public TScanCloseParamsTupleScheme getScheme() {
      return new TScanCloseParamsTupleScheme();
    }
  }

  private static class TScanCloseParamsTupleScheme extends TupleScheme<TScanCloseParams> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TScanCloseParams struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetContext_id()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetContext_id()) {
        oprot.writeString(struct.context_id);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TScanCloseParams struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.context_id = iprot.readString();
        struct.setContext_idIsSet(true);
      }
    }
  }

}

