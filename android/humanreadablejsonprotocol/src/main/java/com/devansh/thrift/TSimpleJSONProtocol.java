package com.devansh.thrift;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Stack;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;

// This class is the same as the Thrift TSimpleJSONProtocol
// with the only difference being in WriteBool where it outputs
// true/false instead of 1 and 0
// Could not extend it since the
// writeContext_.write()
// method is protected
public class TSimpleJSONProtocol extends TProtocol {
    private static final byte[] COMMA = new byte[] { (byte) 44 };
    private static final byte[] COLON = new byte[] { (byte) 58 };
    private static final byte[] LBRACE = new byte[] { (byte) 123 };
    private static final byte[] RBRACE = new byte[] { (byte) 125 };
    private static final byte[] LBRACKET = new byte[] { (byte) 91 };
    private static final byte[] RBRACKET = new byte[] { (byte) 93 };
    private static final char QUOTE = '\"';
    private static final TStruct ANONYMOUS_STRUCT = new TStruct();
    private static final TField ANONYMOUS_FIELD = new TField();
    private static final TMessage EMPTY_MESSAGE = new TMessage();
    private static final TSet EMPTY_SET = new TSet();
    private static final TList EMPTY_LIST = new TList();
    private static final TMap EMPTY_MAP = new TMap();
    private static final String LIST = "list";
    private static final String SET = "set";
    private static final String MAP = "map";
    protected final TSimpleJSONProtocol.Context BASE_CONTEXT = new TSimpleJSONProtocol.Context();
    protected Stack<TSimpleJSONProtocol.Context> writeContextStack_ = new Stack();
    protected TSimpleJSONProtocol.Context writeContext_;

    protected void pushWriteContext(TSimpleJSONProtocol.Context c) {
        this.writeContextStack_.push(this.writeContext_);
        this.writeContext_ = c;
    }

    protected void popWriteContext() {
        this.writeContext_ = (TSimpleJSONProtocol.Context) this.writeContextStack_.pop();
    }

    protected void assertContextIsNotMapKey(
            String invalidKeyType) throws TSimpleJSONProtocol.CollectionMapKeyException {
        if (this.writeContext_.isMapKey()) {
            throw new TSimpleJSONProtocol.CollectionMapKeyException(
                    "Cannot serialize a map with keys that are of type " + invalidKeyType);
        }
    }

    public TSimpleJSONProtocol(TTransport trans) {
        super(trans);
        this.writeContext_ = this.BASE_CONTEXT;
    }

    public void writeMessageBegin(TMessage message) throws TException {
        this.trans_.write(LBRACKET);
        this.pushWriteContext(new TSimpleJSONProtocol.ListContext());
        this.writeString(message.name);
        this.writeByte(message.type);
        this.writeI32(message.seqid);
    }

    public void writeMessageEnd() throws TException {
        this.popWriteContext();
        this.trans_.write(RBRACKET);
    }

    public void writeStructBegin(TStruct struct) throws TException {
        this.writeContext_.write();
        this.trans_.write(LBRACE);
        this.pushWriteContext(new TSimpleJSONProtocol.StructContext());
    }

    public void writeStructEnd() throws TException {
        this.popWriteContext();
        this.trans_.write(RBRACE);
    }

    public void writeFieldBegin(TField field) throws TException {
        this.writeString(field.name);
    }

    public void writeFieldEnd() {
    }

    public void writeFieldStop() {
    }

    public void writeMapBegin(TMap map) throws TException {
        this.assertContextIsNotMapKey("map");
        this.writeContext_.write();
        this.trans_.write(LBRACE);
        this.pushWriteContext(new TSimpleJSONProtocol.MapContext());
    }

    public void writeMapEnd() throws TException {
        this.popWriteContext();
        this.trans_.write(RBRACE);
    }

    public void writeListBegin(TList list) throws TException {
        this.assertContextIsNotMapKey("list");
        this.writeContext_.write();
        this.trans_.write(LBRACKET);
        this.pushWriteContext(new TSimpleJSONProtocol.ListContext());
    }

    public void writeListEnd() throws TException {
        this.popWriteContext();
        this.trans_.write(RBRACKET);
    }

    public void writeSetBegin(TSet set) throws TException {
        this.assertContextIsNotMapKey("set");
        this.writeContext_.write();
        this.trans_.write(LBRACKET);
        this.pushWriteContext(new TSimpleJSONProtocol.ListContext());
    }

    public void writeSetEnd() throws TException {
        this.popWriteContext();
        this.trans_.write(RBRACKET);
    }

    public void writeBool(boolean b) throws TException {
        this.writeContext_.write();
        this._writeStringData(Boolean.toString(b));
    }

    public void writeByte(byte b) throws TException {
        this.writeI32(b);
    }

    public void writeI16(short i16) throws TException {
        this.writeI32(i16);
    }

    public void writeI32(int i32) throws TException {
        if (this.writeContext_.isMapKey()) {
            this.writeString(Integer.toString(i32));
        } else {
            this.writeContext_.write();
            this._writeStringData(Integer.toString(i32));
        }

    }

    public void _writeStringData(String s) throws TException {
        try {
            byte[] uex = s.getBytes("UTF-8");
            this.trans_.write(uex);
        } catch (UnsupportedEncodingException var3) {
            throw new TException("JVM DOES NOT SUPPORT UTF-8");
        }
    }

    public void writeI64(long i64) throws TException {
        if (this.writeContext_.isMapKey()) {
            this.writeString(Long.toString(i64));
        } else {
            this.writeContext_.write();
            this._writeStringData(Long.toString(i64));
        }

    }

    public void writeDouble(double dub) throws TException {
        if (this.writeContext_.isMapKey()) {
            this.writeString(Double.toString(dub));
        } else {
            this.writeContext_.write();
            this._writeStringData(Double.toString(dub));
        }

    }

    public void writeString(String str) throws TException {
        this.writeContext_.write();
        int length = str.length();
        StringBuffer escape = new StringBuffer(length + 16);
        escape.append('\"');

        for (int i = 0; i < length; ++i) {
            char c = str.charAt(i);
            String hex;
            int j;
            switch (c) {
                case '\b':
                    escape.append('\\');
                    escape.append('b');
                    continue;
                case '\t':
                    escape.append('\\');
                    escape.append('t');
                    continue;
                case '\n':
                    escape.append('\\');
                    escape.append('n');
                    continue;
                case '\f':
                    escape.append('\\');
                    escape.append('f');
                    continue;
                case '\r':
                    escape.append('\\');
                    escape.append('r');
                    continue;
                case '\"':
                case '\\':
                    escape.append('\\');
                    escape.append(c);
                    continue;
                default:
                    if (c >= 32) {
                        escape.append(c);
                        continue;
                    }

                    hex = Integer.toHexString(c);
                    escape.append('\\');
                    escape.append('u');
                    j = 4;
            }

            while (j > hex.length()) {
                escape.append('0');
                --j;
            }

            escape.append(hex);
        }

        escape.append('\"');
        this._writeStringData(escape.toString());
    }

    public void writeBinary(ByteBuffer bin) throws TException {
        try {
            this.writeString(new String(bin.array(), bin.position() + bin.arrayOffset(),
                    bin.limit() - bin.position() - bin.arrayOffset(), "UTF-8"));
        } catch (UnsupportedEncodingException var3) {
            throw new TException("JVM DOES NOT SUPPORT UTF-8");
        }
    }

    public TMessage readMessageBegin() throws TException {
        return EMPTY_MESSAGE;
    }

    public void readMessageEnd() {
    }

    public TStruct readStructBegin() {
        return ANONYMOUS_STRUCT;
    }

    public void readStructEnd() {
    }

    public TField readFieldBegin() throws TException {
        return ANONYMOUS_FIELD;
    }

    public void readFieldEnd() {
    }

    public TMap readMapBegin() throws TException {
        return EMPTY_MAP;
    }

    public void readMapEnd() {
    }

    public TList readListBegin() throws TException {
        return EMPTY_LIST;
    }

    public void readListEnd() {
    }

    public TSet readSetBegin() throws TException {
        return EMPTY_SET;
    }

    public void readSetEnd() {
    }

    public boolean readBool() throws TException {
        return this.readByte() == 1;
    }

    public byte readByte() throws TException {
        return (byte) 0;
    }

    public short readI16() throws TException {
        return (short) 0;
    }

    public int readI32() throws TException {
        return 0;
    }

    public long readI64() throws TException {
        return 0L;
    }

    public double readDouble() throws TException {
        return 0.0D;
    }

    public String readString() throws TException {
        return "";
    }

    public String readStringBody(int size) throws TException {
        return "";
    }

    public ByteBuffer readBinary() throws TException {
        return ByteBuffer.wrap(new byte[0]);
    }

    public static class CollectionMapKeyException extends TException {
        public CollectionMapKeyException(String message) {
            super(message);
        }
    }

    protected class MapContext extends TSimpleJSONProtocol.StructContext {
        protected boolean isKey = true;

        protected MapContext() {
            super();
        }

        protected void write() throws TException {
            super.write();
            this.isKey = !this.isKey;
        }

        protected boolean isMapKey() {
            return this.isKey;
        }
    }

    protected class StructContext extends TSimpleJSONProtocol.Context {
        protected boolean first_ = true;
        protected boolean colon_ = true;

        protected StructContext() {
            super();
        }

        protected void write() throws TException {
            if (this.first_) {
                this.first_ = false;
                this.colon_ = true;
            } else {
                TSimpleJSONProtocol.this.trans_
                        .write(this.colon_ ? TSimpleJSONProtocol.COLON : TSimpleJSONProtocol.COMMA);
                this.colon_ = !this.colon_;
            }

        }
    }

    protected class ListContext extends TSimpleJSONProtocol.Context {
        protected boolean first_ = true;

        protected ListContext() {
            super();
        }

        protected void write() throws TException {
            if (this.first_) {
                this.first_ = false;
            } else {
                TSimpleJSONProtocol.this.trans_.write(TSimpleJSONProtocol.COMMA);
            }

        }
    }

    protected class Context {
        protected Context() {
        }

        protected void write() throws TException {
        }

        protected boolean isMapKey() {
            return false;
        }
    }

    public static class Factory implements TProtocolFactory {
        public Factory() {
        }

        public TProtocol getProtocol(TTransport trans) {
            return new TSimpleJSONProtocol(trans);
        }
    }
}
