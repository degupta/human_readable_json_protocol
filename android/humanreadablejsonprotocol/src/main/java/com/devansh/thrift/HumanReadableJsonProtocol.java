package com.devansh.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;
import org.json.JSONArray;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class HumanReadableJsonProtocol extends TProtocol {

    private static final String METHOD_KEY = "method";
    private static final String SERVICES_KEY = "services";
    private static final String NAME_KEY = "name";
    private static final String ARGUMENTS_KEY = "arguments";
    private static final String RESULT_KEY = "result";
    private static final String SUCCESS_KEY = "success";
    private static final String EXCEPTIONS_KEY = "exceptions";
    private static final String EXCEPTION_KEY = "exception";
    private static final String FUNCTIONS_KEY = "functions";
    private static final String KEY_KEY = "key";
    private static final String ONEWAY_KEY = "oneway";
    private static final String FIELDS_KEY = "fields";
    private static final String STRUCTS_KEY = "structs";
    private static final String MESSAGE_KEY = "message";
    private static final String CLASS_KEY = "class";
    private static final String TYPE_ID_KEY = "typeId";
    private static final String TYPE_KEY = "type";
    private static final String KEY_TYPE_ID_KEY = "keyTypeId";
    private static final String KEY_TYPE_KEY = "keyType";
    private static final String VALUE_TYPE_ID_KEY = "valueTypeId";
    private static final String VALUE_TYPE_KEY = "valueType";
    private static final String ELEM_TYPE_ID_KEY = "elemTypeId";
    private static final String ELEM_TYPE_KEY = "elemType"
    private static final String RETURN_TYPE_ID_KEY = "returnTypeId";
    private static final String RETURN_TYPE_KEY = "returnType";

    private JSONArray metadata;
    private String service;
    private LinkedList<Object> params;
    private TSimpleJSONProtocol oprot;

    public HumanReadableJsonProtocol(TTransport transport, JSONArray metadata, String service) {
        super(transport);
        this.metadata = metadata;
        this.service = service;
        this.params = new LinkedList<>();
        oprot = new TSimpleJSONProtocol(transport);
    }

    @Override
    public void writeMessageBegin(TMessage tMessage) throws TException {
        oprot.writeStructBegin(null);
        oprot.writeString(METHOD_KEY);
        oprot.writeString(tMessage.name);
        switch (tMessage.type) {
            case TMessageType.CALL:
                oprot.writeString(ARGUMENTS_KEY);
                break;
            case TMessageType.REPLY:
                oprot.writeString(RESULT_KEY);
                break;
            case TMessageType.EXCEPTION:
                oprot.writeString(EXCEPTION_KEY);
                break;
        }
    }

    @Override
    public void writeMessageEnd() throws TException {
        oprot.writeStructEnd();
    }

    @Override
    public void writeStructBegin(TStruct tStruct) throws TException {
        oprot.writeStructBegin(tStruct);
    }

    @Override
    public void writeStructEnd() throws TException {
        oprot.writeStructEnd();
    }

    @Override
    public void writeFieldBegin(TField tField) throws TException {
        oprot.writeString(tField.name);
    }

    @Override
    public void writeFieldEnd() throws TException {
        // No-op
    }

    @Override
    public void writeFieldStop() throws TException {
        // No-op
    }

    @Override
    public void writeMapBegin(TMap tMap) throws TException {
        oprot.writeStructBegin(null);
    }

    @Override
    public void writeMapEnd() throws TException {
        oprot.writeStructEnd();
    }

    @Override
    public void writeListBegin(TList tList) throws TException {
        oprot.writeListBegin(tList);
    }

    @Override
    public void writeListEnd() throws TException {
        oprot.writeListEnd();
    }

    @Override
    public void writeSetBegin(TSet tSet) throws TException {
        oprot.writeSetBegin(tSet);
    }

    @Override
    public void writeSetEnd() throws TException {
        oprot.writeStructEnd();
    }


    @Override
    public void writeBool(boolean b) throws TException {
        oprot.writeBool(b);
    }

    @Override
    public void writeByte(byte b) throws TException {
        oprot.writeByte(b);
    }

    @Override
    public void writeI16(short i) throws TException {
        oprot.writeI16(i);
    }

    @Override
    public void writeI32(int i) throws TException {
        oprot.writeI32(i);
    }

    @Override
    public void writeI64(long l) throws TException {
        oprot.writeI64(l);
    }

    @Override
    public void writeDouble(double v) throws TException {
        oprot.writeDouble(v);
    }

    @Override
    public void writeString(String s) throws TException {
        oprot.writeString(s);
    }

    @Override
    public void writeBinary(ByteBuffer byteBuffer) throws TException {
        oprot.writeBinary(byteBuffer);
    }

    @Override
    public TMessage readMessageBegin() throws TException {
        return null;
    }

    @Override
    public void readMessageEnd() throws TException {

    }

    @Override
    public TStruct readStructBegin() throws TException {
        return null;
    }

    @Override
    public void readStructEnd() throws TException {

    }

    @Override
    public TField readFieldBegin() throws TException {
        return null;
    }

    @Override
    public void readFieldEnd() throws TException {

    }

    @Override
    public TMap readMapBegin() throws TException {
        return null;
    }

    @Override
    public void readMapEnd() throws TException {

    }

    @Override
    public TList readListBegin() throws TException {
        return null;
    }

    @Override
    public void readListEnd() throws TException {

    }

    @Override
    public TSet readSetBegin() throws TException {
        return null;
    }

    @Override
    public void readSetEnd() throws TException {

    }

    @Override
    public boolean readBool() throws TException {
        return false;
    }

    @Override
    public byte readByte() throws TException {
        return 0;
    }

    @Override
    public short readI16() throws TException {
        return 0;
    }

    @Override
    public int readI32() throws TException {
        return 0;
    }

    @Override
    public long readI64() throws TException {
        return 0;
    }

    @Override
    public double readDouble() throws TException {
        return 0;
    }

    @Override
    public String readString() throws TException {
        return null;
    }

    @Override
    public ByteBuffer readBinary() throws TException {
        return null;
    }
}
