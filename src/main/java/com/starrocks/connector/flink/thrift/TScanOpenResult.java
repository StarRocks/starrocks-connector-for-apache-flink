/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.starrocks.connector.flink.thrift;

import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TScanOpenResult implements org.apache.thrift.TBase<TScanOpenResult, TScanOpenResult._Fields>, java.io.Serializable, Cloneable, Comparable<TScanOpenResult> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TScanOpenResult");

  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField CONTEXT_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("context_id", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField SELECTED_COLUMNS_FIELD_DESC = new org.apache.thrift.protocol.TField("selected_columns", org.apache.thrift.protocol.TType.LIST, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TScanOpenResultStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TScanOpenResultTupleSchemeFactory());
  }

  public com.starrocks.connector.flink.thrift.TStatus status; // required
  public String context_id; // optional
  public List<TScanColumnDesc> selected_columns; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STATUS((short)1, "status"),
    CONTEXT_ID((short)2, "context_id"),
    SELECTED_COLUMNS((short)3, "selected_columns");

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
        case 1: // STATUS
          return STATUS;
        case 2: // CONTEXT_ID
          return CONTEXT_ID;
        case 3: // SELECTED_COLUMNS
          return SELECTED_COLUMNS;
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
  private _Fields optionals[] = {_Fields.CONTEXT_ID,_Fields.SELECTED_COLUMNS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, com.starrocks.connector.flink.thrift.TStatus.class)));
    tmpMap.put(_Fields.CONTEXT_ID, new org.apache.thrift.meta_data.FieldMetaData("context_id", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SELECTED_COLUMNS, new org.apache.thrift.meta_data.FieldMetaData("selected_columns", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TScanColumnDesc.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TScanOpenResult.class, metaDataMap);
  }

  public TScanOpenResult() {
  }

  public TScanOpenResult(
    com.starrocks.connector.flink.thrift.TStatus status)
  {
    this();
    this.status = status;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TScanOpenResult(TScanOpenResult other) {
    if (other.isSetStatus()) {
      this.status = new com.starrocks.connector.flink.thrift.TStatus(other.status);
    }
    if (other.isSetContext_id()) {
      this.context_id = other.context_id;
    }
    if (other.isSetSelected_columns()) {
      List<TScanColumnDesc> __this__selected_columns = new ArrayList<TScanColumnDesc>(other.selected_columns.size());
      for (TScanColumnDesc other_element : other.selected_columns) {
        __this__selected_columns.add(new TScanColumnDesc(other_element));
      }
      this.selected_columns = __this__selected_columns;
    }
  }

  public TScanOpenResult deepCopy() {
    return new TScanOpenResult(this);
  }

  @Override
  public void clear() {
    this.status = null;
    this.context_id = null;
    this.selected_columns = null;
  }

  public com.starrocks.connector.flink.thrift.TStatus getStatus() {
    return this.status;
  }

  public TScanOpenResult setStatus(com.starrocks.connector.flink.thrift.TStatus status) {
    this.status = status;
    return this;
  }

  public void unsetStatus() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  public String getContext_id() {
    return this.context_id;
  }

  public TScanOpenResult setContext_id(String context_id) {
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

  public int getSelected_columnsSize() {
    return (this.selected_columns == null) ? 0 : this.selected_columns.size();
  }

  public java.util.Iterator<TScanColumnDesc> getSelected_columnsIterator() {
    return (this.selected_columns == null) ? null : this.selected_columns.iterator();
  }

  public void addToSelected_columns(TScanColumnDesc elem) {
    if (this.selected_columns == null) {
      this.selected_columns = new ArrayList<TScanColumnDesc>();
    }
    this.selected_columns.add(elem);
  }

  public List<TScanColumnDesc> getSelected_columns() {
    return this.selected_columns;
  }

  public TScanOpenResult setSelected_columns(List<TScanColumnDesc> selected_columns) {
    this.selected_columns = selected_columns;
    return this;
  }

  public void unsetSelected_columns() {
    this.selected_columns = null;
  }

  /** Returns true if field selected_columns is set (has been assigned a value) and false otherwise */
  public boolean isSetSelected_columns() {
    return this.selected_columns != null;
  }

  public void setSelected_columnsIsSet(boolean value) {
    if (!value) {
      this.selected_columns = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((com.starrocks.connector.flink.thrift.TStatus)value);
      }
      break;

    case CONTEXT_ID:
      if (value == null) {
        unsetContext_id();
      } else {
        setContext_id((String)value);
      }
      break;

    case SELECTED_COLUMNS:
      if (value == null) {
        unsetSelected_columns();
      } else {
        setSelected_columns((List<TScanColumnDesc>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS:
      return getStatus();

    case CONTEXT_ID:
      return getContext_id();

    case SELECTED_COLUMNS:
      return getSelected_columns();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case STATUS:
      return isSetStatus();
    case CONTEXT_ID:
      return isSetContext_id();
    case SELECTED_COLUMNS:
      return isSetSelected_columns();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TScanOpenResult)
      return this.equals((TScanOpenResult)that);
    return false;
  }

  public boolean equals(TScanOpenResult that) {
    if (that == null)
      return false;

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_context_id = true && this.isSetContext_id();
    boolean that_present_context_id = true && that.isSetContext_id();
    if (this_present_context_id || that_present_context_id) {
      if (!(this_present_context_id && that_present_context_id))
        return false;
      if (!this.context_id.equals(that.context_id))
        return false;
    }

    boolean this_present_selected_columns = true && this.isSetSelected_columns();
    boolean that_present_selected_columns = true && that.isSetSelected_columns();
    if (this_present_selected_columns || that_present_selected_columns) {
      if (!(this_present_selected_columns && that_present_selected_columns))
        return false;
      if (!this.selected_columns.equals(that.selected_columns))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TScanOpenResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
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
    lastComparison = Boolean.valueOf(isSetSelected_columns()).compareTo(other.isSetSelected_columns());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSelected_columns()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.selected_columns, other.selected_columns);
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
    StringBuilder sb = new StringBuilder("TScanOpenResult(");
    boolean first = true;

    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (isSetContext_id()) {
      if (!first) sb.append(", ");
      sb.append("context_id:");
      if (this.context_id == null) {
        sb.append("null");
      } else {
        sb.append(this.context_id);
      }
      first = false;
    }
    if (isSetSelected_columns()) {
      if (!first) sb.append(", ");
      sb.append("selected_columns:");
      if (this.selected_columns == null) {
        sb.append("null");
      } else {
        sb.append(this.selected_columns);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (status == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
    if (status != null) {
      status.validate();
    }
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

  private static class TScanOpenResultStandardSchemeFactory implements SchemeFactory {
    public TScanOpenResultStandardScheme getScheme() {
      return new TScanOpenResultStandardScheme();
    }
  }

  private static class TScanOpenResultStandardScheme extends StandardScheme<TScanOpenResult> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TScanOpenResult struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.status = new com.starrocks.connector.flink.thrift.TStatus();
              struct.status.read(iprot);
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // CONTEXT_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.context_id = iprot.readString();
              struct.setContext_idIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SELECTED_COLUMNS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list18 = iprot.readListBegin();
                struct.selected_columns = new ArrayList<TScanColumnDesc>(_list18.size);
                for (int _i19 = 0; _i19 < _list18.size; ++_i19)
                {
                  TScanColumnDesc _elem20;
                  _elem20 = new TScanColumnDesc();
                  _elem20.read(iprot);
                  struct.selected_columns.add(_elem20);
                }
                iprot.readListEnd();
              }
              struct.setSelected_columnsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TScanOpenResult struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        struct.status.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.context_id != null) {
        if (struct.isSetContext_id()) {
          oprot.writeFieldBegin(CONTEXT_ID_FIELD_DESC);
          oprot.writeString(struct.context_id);
          oprot.writeFieldEnd();
        }
      }
      if (struct.selected_columns != null) {
        if (struct.isSetSelected_columns()) {
          oprot.writeFieldBegin(SELECTED_COLUMNS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.selected_columns.size()));
            for (TScanColumnDesc _iter21 : struct.selected_columns)
            {
              _iter21.write(oprot);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TScanOpenResultTupleSchemeFactory implements SchemeFactory {
    public TScanOpenResultTupleScheme getScheme() {
      return new TScanOpenResultTupleScheme();
    }
  }

  private static class TScanOpenResultTupleScheme extends TupleScheme<TScanOpenResult> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TScanOpenResult struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.status.write(oprot);
      BitSet optionals = new BitSet();
      if (struct.isSetContext_id()) {
        optionals.set(0);
      }
      if (struct.isSetSelected_columns()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetContext_id()) {
        oprot.writeString(struct.context_id);
      }
      if (struct.isSetSelected_columns()) {
        {
          oprot.writeI32(struct.selected_columns.size());
          for (TScanColumnDesc _iter22 : struct.selected_columns)
          {
            _iter22.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TScanOpenResult struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.status = new com.starrocks.connector.flink.thrift.TStatus();
      struct.status.read(iprot);
      struct.setStatusIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.context_id = iprot.readString();
        struct.setContext_idIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list23 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.selected_columns = new ArrayList<TScanColumnDesc>(_list23.size);
          for (int _i24 = 0; _i24 < _list23.size; ++_i24)
          {
            TScanColumnDesc _elem25;
            _elem25 = new TScanColumnDesc();
            _elem25.read(iprot);
            struct.selected_columns.add(_elem25);
          }
        }
        struct.setSelected_columnsIsSet(true);
      }
    }
  }

}

