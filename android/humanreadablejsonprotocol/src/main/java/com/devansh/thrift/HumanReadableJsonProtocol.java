package com.devansh.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Iterator;
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
    private static final String ELEM_TYPE_KEY = "elemType";
    private static final String RETURN_TYPE_ID_KEY = "returnTypeId";
    private static final String RETURN_TYPE_KEY = "returnType";

    private JSONArray metadata;
    private String service;
    private LinkedList<Object> params;
    private TSimpleJSONProtocol oprot;
    private TException err;

    public HumanReadableJsonProtocol(TTransport transport, JSONArray metadata, String service) {
        super(transport);
        this.metadata = metadata;
        this.service = service;
        this.params = new LinkedList<>();
        oprot = new TSimpleJSONProtocol(transport);
    }

    private JSONObject findInJsonArray(JSONArray arr, String key,
                                       String value) throws JSONException {
        for (int i = 0; i < arr.length(); i++) {
            JSONObject obj = arr.getJSONObject(i);
            if (value.equals(obj.getString(key))) {
                return obj;
            }
        }

        return null;
    }

    private JSONObject getMethodInfo(String serviceName, String methodName) throws JSONException {
        for (int i = 0; i < metadata.length(); i++) {
            JSONObject v = metadata.getJSONObject(i);
            if (v.has(SERVICES_KEY)) {
                JSONObject svc = findInJsonArray(v.getJSONArray(SERVICES_KEY), NAME_KEY,
                        v.getString(NAME_KEY) + "." + serviceName);
                if (svc != null) {
                    JSONObject methodInfo =
                            findInJsonArray(svc.getJSONArray(FUNCTIONS_KEY), NAME_KEY, methodName);
                    if (methodInfo != null) {
                        return methodInfo;
                    }
                }
            }
        }

        return null;
    }

    private byte[] getMessageTypeAndSeq(JSONObject request,
                                        JSONObject methodInfo) throws TProtocolException {
        if (request.has(ARGUMENTS_KEY)) {
            if (methodInfo != null && methodInfo.has((ONEWAY_KEY))) {
                return new byte[] { TMessageType.ONEWAY, 0 };
            } else {
                return new byte[] { TMessageType.CALL, 0 };
            }
        } else if (request.has(RESULT_KEY)) {
            return new byte[] { TMessageType.REPLY, 1 };
        } else if (request.has(EXCEPTION_KEY)) {
            return new byte[] { TMessageType.EXCEPTION, 1 };
        } else {
            throw new TProtocolException(TProtocolException.INVALID_DATA,
                    new Exception("Unable to parse message type"));
        }
    }

    private void add(Object... vals) {
        for (Object o : vals) {
            params.add(o);
        }
    }

    private JSONObject getInfo(String clazz) throws JSONException {
        String[] parts = clazz.split(".");
        JSONObject program = findInJsonArray(metadata, NAME_KEY, parts[0]);
        return findInJsonArray(program.getJSONArray(STRUCTS_KEY), NAME_KEY, parts[1]);
    }

    private JSONArray getStructFieldList(JSONObject elemType) throws JSONException {
        return getInfo(elemType.getString(CLASS_KEY)).getJSONArray(FIELDS_KEY);
    }

    private void raiseExpected(String type, Object got) throws TProtocolException {
        throw new TProtocolException(TProtocolException.INVALID_DATA, new Exception(
                "Expected " + type + " got " +
                        (got == null ? "null" : got.getClass().getSimpleName())));
    }

    private void parse(JSONObject fieldInfo, Object value, String fieldTypeIdKey,
                       String fieldTypeKey) throws JSONException, TProtocolException {
        String fieldType = fieldInfo.getString(fieldTypeIdKey);

        switch (fieldType) {
            case "bool":
                if (value instanceof Boolean) {
                    add(value);
                } else {
                    raiseExpected("bool", value);
                }
                break;

            case "i8":
            case "i16":
            case "i32":
            case "i64":
            case "double":
                if (value instanceof Number) {
                    add(value);
                } else {
                    raiseExpected(fieldType, value);
                }
                break;

            case "string":
                if (value instanceof String) {
                    add(value);
                } else {
                    raiseExpected("string", value);
                }
                break;

            case "struct":
            case "union":
            case "exception":
                parseStruct(getStructFieldList(fieldInfo.getJSONObject(fieldTypeKey)), value);
                break;

            case "map":
                parseMap(fieldInfo.getJSONObject(fieldTypeKey), value);
                break;

            case "set":
            case "list":
                parseList(fieldInfo.getJSONObject(fieldTypeKey), value);
                break;

            default:
                throw new TProtocolException(TProtocolException.INVALID_DATA,
                        new Exception("Unexpected type " + fieldType));
        }
    }

    private void parseMap(JSONObject fieldInfo,
                          Object request) throws TProtocolException, JSONException {
        if (!(request instanceof JSONObject)) {
            raiseExpected("JSON Object", request);
        }
        JSONObject jsonObject = (JSONObject) request;

        byte keyType = stringToTypeId(fieldInfo.getString(KEY_TYPE_ID_KEY));
        byte valueType = stringToTypeId(fieldInfo.getString(VALUE_TYPE_ID_KEY));
        add(keyType, valueType, jsonObject.length());

        Iterator<String> it = jsonObject.keys();
        while (it.hasNext()) {
            String key = it.next();
            Object value = jsonObject.get(key);
            parse(fieldInfo, key, KEY_TYPE_ID_KEY, KEY_TYPE_KEY);
            parse(fieldInfo, value, VALUE_TYPE_ID_KEY, VALUE_TYPE_KEY);
        }
    }

    private void parseList(JSONObject fieldInfo,
                           Object request) throws TProtocolException, JSONException {
        if (!(request instanceof JSONArray)) {
            raiseExpected("JSON Array", request);
        }
        JSONArray jsonArray = (JSONArray) request;

        byte elemType = stringToTypeId(fieldInfo.getString(ELEM_TYPE_ID_KEY));
        add(elemType, jsonArray.length());

        for (int i = 0; i < jsonArray.length(); i++) {
            parse(fieldInfo, jsonArray.get(i), ELEM_TYPE_ID_KEY, ELEM_TYPE_KEY);
        }
    }

    public void parseStruct(JSONArray fieldsList,
                            Object request) throws TProtocolException, JSONException {
        if (!(request instanceof JSONObject)) {
            raiseExpected("JSON Object", request);
        }
        JSONObject jsonObject = (JSONObject) request;

        Iterator<String> it = jsonObject.keys();
        while (it.hasNext()) {
            String key = it.next();
            Object value = jsonObject.get(key);
            JSONObject fieldInfo = findInJsonArray(fieldsList, NAME_KEY, key);
            if (fieldInfo == null) {
                throw new TProtocolException(TProtocolException.INVALID_DATA,
                        new Exception("Unexpected key " + key));
            }

            byte fieldType = stringToTypeId(fieldInfo.getString(TYPE_ID_KEY));
            add(key, fieldType, fieldInfo.optInt(KEY_KEY, 0));
            parse(fieldInfo, value, TYPE_ID_KEY, TYPE_KEY);
            add("", TType.STOP, -1);
        }
    }

    private byte stringToTypeId(String fieldType) throws TProtocolException {
        switch (fieldType) {
            case "bool":
                return TType.BOOL;
            case "i8":
                return TType.BYTE;
            case "i16":
                return TType.I16;
            case "i32":
                return TType.I32;
            case "i64":
                return TType.I64;
            case "double":
                return TType.DOUBLE;
            case "string":
                return TType.STRING;
            case "struct":
            case "union":
            case "exception":
                return TType.STRUCT;
            case "map":
                return TType.MAP;
            case "set":
                return TType.SET;
            case "list":
                return TType.LIST;
            default:
                throw new TProtocolException(TProtocolException.INVALID_DATA,
                        new Exception("Unknown type identifier " + fieldType));
        }
    }

    @Override
    public TMessage readMessageBegin() throws TException {
        try {
            return readMessageBeginHelper();
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    private TMessage readMessageBeginHelper() throws JSONException, TProtocolException, IOException {
        StringWriter writer = new StringWriter();
        char[] buffer = new char[1028];
        InputStreamReader input = new InputStreamReader(new InputStream() {
            private byte[] buf = new byte[1];

            @Override
            public int read() throws IOException {
                try {
                    int amt = getTransport().read(buf, 0, 1);
                    if (amt <= 0) {
                        return -1;
                    } else {
                        return buf[0] & 0xFF;
                    }
                } catch (TTransportException e) {
                    return -1;
                }
            }

        }, "UTF-8");

        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            writer.write(buffer, 0, n);
        }

        JSONObject request = new JSONObject(writer.toString());

        String name = request.getString(METHOD_KEY);
        JSONObject methodInfo = getMethodInfo(service, name);
        byte[] messageTypeAndSeq = getMessageTypeAndSeq(request, methodInfo);
        byte typeId = messageTypeAndSeq[0];
        byte seqId = messageTypeAndSeq[1];

        if (request.has(ARGUMENTS_KEY)) {
            if (methodInfo == null) {
                add("", TType.STOP, -1);
                return new TMessage(name, typeId, seqId);
            }

            parseStruct(methodInfo.getJSONArray(ARGUMENTS_KEY), request.get(ARGUMENTS_KEY));
        } else if (request.has(RESULT_KEY)) {
            if (methodInfo == null) {
                add("", TType.STOP, -1);
                return new TMessage(name, typeId, seqId);
            }

            JSONObject result = request.getJSONObject(RESULT_KEY);
            if (result.has(SUCCESS_KEY)) {
                try {
                    byte returnType = stringToTypeId(methodInfo.getString(RETURN_TYPE_ID_KEY));
                    add("", returnType, 0);
                    parse(methodInfo, result.get(SUCCESS_KEY), RETURN_TYPE_ID_KEY, RETURN_TYPE_KEY);
                    add("", TType.STOP, -1);
                } catch (Exception e) {
                    err = new TException(e);
                }
            } else if (result.length() == 0) {
                add("", TType.STOP, -1);
            } else {
                String errName = result.keys().next();
                JSONObject errInfo =
                        findInJsonArray(methodInfo.getJSONArray(EXCEPTIONS_KEY), NAME_KEY, errName);
                if (errInfo == null) {
                    throw new TProtocolException(TProtocolException.INVALID_DATA,
                            new Exception("Unable to parse result"));
                }
                add(errName, TType.STRUCT, errInfo.getInt(KEY_KEY));

                try {
                    parse(errInfo, result.get(errName), TYPE_ID_KEY, TYPE_KEY);
                } catch (Exception e) {
                    err = new TException(e);
                }
                add("", TType.STOP, -1);
            }
        } else if (request.has(EXCEPTION_KEY)) {
            add("", TType.STRING, 1);
            add(request.getJSONObject(EXCEPTION_KEY).optString(MESSAGE_KEY, ""));
            add("", TType.I32, 2);
            add(request.getJSONObject(EXCEPTION_KEY).optInt(TYPE_KEY, TProtocolException.UNKNOWN));
            add("", TType.STOP, -1);
        } else {
            throw new TProtocolException(TProtocolException.INVALID_DATA,
                    new Exception("Unable to parse result"));
        }

        return new TMessage(name, typeId, seqId);
    }

    @Override
    public void readMessageEnd() throws TException {
        //  No-op
    }

    @Override
    public TStruct readStructBegin() throws TException {
        if (err != null) {
            throw err;
        }
        return new TStruct();
    }

    @Override
    public void readStructEnd() throws TException {
        //  No-op
    }

    @Override
    public TField readFieldBegin() throws TException {
        return new TField((String) params.pollFirst(), (Byte) params.pollFirst(),
                (Short) params.pollFirst());
    }

    @Override
    public void readFieldEnd() throws TException {
        //  No-op
    }

    @Override
    public TMap readMapBegin() throws TException {
        return new TMap((Byte) params.pollFirst(), (Byte) params.pollFirst(),
                (Integer) params.pollFirst());
    }

    @Override
    public void readMapEnd() throws TException {
        //  No-op
    }

    @Override
    public TList readListBegin() throws TException {
        return new TList((Byte) params.pollFirst(), (Integer) params.pollFirst());
    }

    @Override
    public void readListEnd() throws TException {
        //  No-op
    }

    @Override
    public TSet readSetBegin() throws TException {
        return new TSet((Byte) params.pollFirst(), (Integer) params.pollFirst());
    }

    @Override
    public void readSetEnd() throws TException {
        //  No-op
    }

    @Override
    public boolean readBool() throws TException {
        return (boolean) params.pollFirst();
    }

    @Override
    public byte readByte() throws TException {
        return ((Number) params.pollFirst()).byteValue();
    }

    @Override
    public short readI16() throws TException {
        return ((Number) params.pollFirst()).shortValue();
    }

    @Override
    public int readI32() throws TException {
        return ((Number) params.pollFirst()).intValue();
    }

    @Override
    public long readI64() throws TException {
        return ((Number) params.pollFirst()).longValue();
    }

    @Override
    public double readDouble() throws TException {
        return ((Number) params.pollFirst()).doubleValue();
    }

    @Override
    public String readString() throws TException {
        return (String) params.pollFirst();
    }

    @Override
    public ByteBuffer readBinary() throws TException {
        return ByteBuffer.wrap(readString().getBytes());
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
}
