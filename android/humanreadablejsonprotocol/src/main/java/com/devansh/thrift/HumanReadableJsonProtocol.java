package com.devansh.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;
import org.json.JSONArray;

import java.nio.ByteBuffer;

public class HumanReadableJsonProtocol extends TProtocol {

    private JSONArray metadata;
    private String service;

    public HumanReadableJsonProtocol(TTransport transport, JSONArray metadata, String service) {
        super(transport);
        this.metadata = metadata;
        this.service = service;
    }

    @Override
    public void writeMessageBegin(TMessage tMessage) throws TException {

    }

    @Override
    public void writeMessageEnd() throws TException {

    }

    @Override
    public void writeStructBegin(TStruct tStruct) throws TException {

    }

    @Override
    public void writeStructEnd() throws TException {

    }

    @Override
    public void writeFieldBegin(TField tField) throws TException {

    }

    @Override
    public void writeFieldEnd() throws TException {

    }

    @Override
    public void writeFieldStop() throws TException {

    }

    @Override
    public void writeMapBegin(TMap tMap) throws TException {

    }

    @Override
    public void writeMapEnd() throws TException {

    }

    @Override
    public void writeListBegin(TList tList) throws TException {

    }

    @Override
    public void writeListEnd() throws TException {

    }

    @Override
    public void writeSetBegin(TSet tSet) throws TException {

    }

    @Override
    public void writeSetEnd() throws TException {

    }

    @Override
    public void writeBool(boolean b) throws TException {

    }

    @Override
    public void writeByte(byte b) throws TException {

    }

    @Override
    public void writeI16(short i) throws TException {

    }

    @Override
    public void writeI32(int i) throws TException {

    }

    @Override
    public void writeI64(long l) throws TException {

    }

    @Override
    public void writeDouble(double v) throws TException {

    }

    @Override
    public void writeString(String s) throws TException {

    }

    @Override
    public void writeBinary(ByteBuffer byteBuffer) throws TException {

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
