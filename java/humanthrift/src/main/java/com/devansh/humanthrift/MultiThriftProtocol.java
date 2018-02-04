package com.devansh.humanthrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * This class allows you to multiplex between different kinds of protocol.
 * Useful when you want to send JSON during Dev/Debugging and Binary/Compact in Production
 * <p>
 * The first character 1,2,3,4 (IN STRING - so 49,50,51,52) correspond to
 * Binary, Compact, Thrift JSON, Human JSON.
 * <p>
 * You can init the type by either calling {@link MultiThriftProtocol#readProtocolTypeFromTransport()}
 * which will read the very next byte to determine the type (ideally the very first byte)
 * <p>
 * or by calling {@link MultiThriftProtocol#setProtocolType(String)} which write the protocol type
 * to underlying transport.
 * <p>
 * It's a String and not a byte since it would hard to send 1,2,3,4 in JSON.
 * <p>
 * Sample Request:
 * 4{"m":"askMe","a": {"question": "Something Something"}}
 * <p>
 * Response:
 * 4{"m":"askMe","r":{"success":"42"}}
 * <p>
 * (notice '4' upfront in both Request and Response)
 */
public class MultiThriftProtocol extends TProtocol {

    private static final Logger log = LoggerFactory.getLogger(MultiThriftProtocol.class);

    public static class Factory implements TProtocolFactory {

        private final JSONArray metadata;
        private final String serviceName;

        public Factory(JSONArray metadata, String serviceName) {
            this.metadata = metadata;
            this.serviceName = serviceName;
        }


        @Override
        public TProtocol getProtocol(TTransport trans) {
            return new MultiThriftProtocol(trans, metadata, serviceName);
        }
    }

    private static final String BINARY_IDX = "1";
    private static final String COMPACT_IDX = "2";
    private static final String THRIFT_JSON_IDX = "3";
    private static final String JSON_IDX = "4";

    private TProtocol delegatedProtocol;
    private final String serviceName;
    private final JSONArray metadata;

    public MultiThriftProtocol(TTransport transport, JSONArray metadata, String serviceName) {
        super(transport);
        this.metadata = metadata;
        this.serviceName = serviceName;
    }

    public void setProtocolType(String protocolType) throws TException {
        delegatedProtocol = getDelegatedProtocol(protocolType);
        trans_.write(protocolType.getBytes());
    }

    public String readProtocolTypeFromTransport() throws TException {
        // Default to Binary
        String delegatedProtocolType = "";
        try {
            byte[] b = new byte[1];
            trans_.read(b, 0, 1);
            delegatedProtocolType = new String(b);
            delegatedProtocol = getDelegatedProtocol(delegatedProtocolType);
        } catch (TTransportException e) {
            log.error("Error Getting Multi Protocol", e);
        }
        return delegatedProtocolType;
    }

    private TProtocol getDelegatedProtocol(String delegatedProtocolType) throws TTransportException {
        TProtocolFactory protocolFactory = null;
        switch (delegatedProtocolType) {
            case BINARY_IDX:
                protocolFactory = new TBinaryProtocol.Factory();
                break;

            case COMPACT_IDX:
                protocolFactory = new TCompactProtocol.Factory();
                break;

            case THRIFT_JSON_IDX:
                protocolFactory = new TJSONProtocol.Factory();
                break;

            case JSON_IDX:
                protocolFactory = new HumanReadableJsonProtocol.Factory(metadata, serviceName);
                break;

            default:
                throw new TTransportException("Unknown type: " + delegatedProtocolType);
        }

        return protocolFactory.getProtocol(trans_);
    }

    @Override
    public void writeMessageBegin(TMessage message) throws TException {
        delegatedProtocol.writeMessageBegin(message);
    }

    @Override
    public void writeMessageEnd() throws TException {
        delegatedProtocol.writeMessageEnd();
    }

    @Override
    public void writeStructBegin(TStruct struct) throws TException {
        delegatedProtocol.writeStructBegin(struct);
    }

    @Override
    public void writeStructEnd() throws TException {
        delegatedProtocol.writeMessageEnd();
    }

    @Override
    public void writeFieldBegin(TField field) throws TException {
        delegatedProtocol.writeFieldBegin(field);
    }

    @Override
    public void writeFieldEnd() throws TException {
        delegatedProtocol.writeFieldEnd();
    }

    @Override
    public void writeFieldStop() throws TException {
        delegatedProtocol.writeFieldStop();
    }

    @Override
    public void writeMapBegin(TMap map) throws TException {
        delegatedProtocol.writeMapBegin(map);
    }

    @Override
    public void writeMapEnd() throws TException {
        delegatedProtocol.writeMapEnd();
    }

    @Override
    public void writeListBegin(TList list) throws TException {
        delegatedProtocol.writeListBegin(list);
    }

    @Override
    public void writeListEnd() throws TException {
        delegatedProtocol.writeListEnd();
    }

    @Override
    public void writeSetBegin(TSet set) throws TException {
        delegatedProtocol.writeSetBegin(set);
    }

    @Override
    public void writeSetEnd() throws TException {
        delegatedProtocol.writeSetEnd();
    }

    @Override
    public void writeBool(boolean b) throws TException {
        delegatedProtocol.writeBool(b);
    }

    @Override
    public void writeByte(byte b) throws TException {
        delegatedProtocol.writeByte(b);
    }

    @Override
    public void writeI16(short i16) throws TException {
        delegatedProtocol.writeI16(i16);
    }

    @Override
    public void writeI32(int i32) throws TException {
        delegatedProtocol.writeI32(i32);
    }

    @Override
    public void writeI64(long i64) throws TException {
        delegatedProtocol.writeI64(i64);
    }

    @Override
    public void writeDouble(double dub) throws TException {
        delegatedProtocol.writeDouble(dub);
    }

    @Override
    public void writeString(String str) throws TException {
        delegatedProtocol.writeString(str);
    }

    @Override
    public void writeBinary(ByteBuffer buf) throws TException {
        delegatedProtocol.writeBinary(buf);
    }

    @Override
    public TMessage readMessageBegin() throws TException {
        return delegatedProtocol.readMessageBegin();
    }

    @Override
    public void readMessageEnd() throws TException {
        delegatedProtocol.readMessageEnd();
    }

    @Override
    public TStruct readStructBegin() throws TException {
        return delegatedProtocol.readStructBegin();
    }

    @Override
    public void readStructEnd() throws TException {
        delegatedProtocol.readStructEnd();
    }

    @Override
    public TField readFieldBegin() throws TException {
        return delegatedProtocol.readFieldBegin();
    }

    @Override
    public void readFieldEnd() throws TException {
        delegatedProtocol.readFieldEnd();
    }

    @Override
    public TMap readMapBegin() throws TException {
        return delegatedProtocol.readMapBegin();
    }

    @Override
    public void readMapEnd() throws TException {
        delegatedProtocol.readMapEnd();
    }

    @Override
    public TList readListBegin() throws TException {
        return delegatedProtocol.readListBegin();
    }

    @Override
    public void readListEnd() throws TException {
        delegatedProtocol.readListEnd();
    }

    @Override
    public TSet readSetBegin() throws TException {
        return delegatedProtocol.readSetBegin();
    }

    @Override
    public void readSetEnd() throws TException {
        delegatedProtocol.readSetEnd();
    }

    @Override
    public boolean readBool() throws TException {
        return delegatedProtocol.readBool();
    }

    @Override
    public byte readByte() throws TException {
        return delegatedProtocol.readByte();
    }

    @Override
    public short readI16() throws TException {
        return delegatedProtocol.readI16();
    }

    @Override
    public int readI32() throws TException {
        return delegatedProtocol.readI32();
    }

    @Override
    public long readI64() throws TException {
        return delegatedProtocol.readI64();
    }

    @Override
    public double readDouble() throws TException {
        return delegatedProtocol.readDouble();
    }

    @Override
    public String readString() throws TException {
        return delegatedProtocol.readString();
    }

    @Override
    public ByteBuffer readBinary() throws TException {
        return delegatedProtocol.readBinary();
    }
}
