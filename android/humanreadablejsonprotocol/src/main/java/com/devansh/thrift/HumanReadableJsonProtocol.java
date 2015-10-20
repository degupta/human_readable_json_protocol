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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
    private static final String ELEM_TYPE_KEY = "elemType"
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

    private JSONObject find_in_json_array(JSONArray arr, String key,
                                          String value) throws JSONException {
        for (int i = 0; i < arr.length(); i++) {
            JSONObject obj = arr.getJSONObject(i);
            if (value.equals(obj.getString(key))) {
                return obj;
            }
        }

        return null;
    }

    private byte[] get_message_type_and_seq(JSONObject request,
                                            JSONObject method_info) throws TProtocolException {
        if (request.has(ARGUMENTS_KEY)) {
            if (method_info != null && method_info.has((ONEWAY_KEY))) {
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

    private JSONObject get_info(String clazz) throws JSONException {
        String[] parts = clazz.split(".");
        JSONObject program = find_in_json_array(metadata, NAME_KEY, parts[0]);
        return find_in_json_array(program.getJSONArray(STRUCTS_KEY), NAME_KEY, parts[1]);
    }

    private JSONArray get_struct_field_list(JSONObject elem_type) throws JSONException {
        return get_info(elem_type.getString(CLASS_KEY)).getJSONArray(FIELDS_KEY);
    }

    private void raise_expected(String type, Object got) throws TProtocolException {
        throw new TProtocolException(TProtocolException.INVALID_DATA, new Exception(
                "Expected " + type + " got " +
                        (got == null ? "null" : got.getClass().getSimpleName())));
    }

    private void parse(JSONObject field_info, Object value, String field_type_id_key,
                       String field_type_key) throws JSONException, TProtocolException {
        String field_type = field_info.getString(field_type_id_key);

        switch (field_type) {
            case "bool":
                if (value instanceof Boolean) {
                    add(value);
                } else {
                    raise_expected("bool", value);
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
                    raise_expected(field_type, value);
                }
                break;

            case "string":
                if (value instanceof String) {
                    add(value);
                } else {
                    raise_expected("string", value);
                }
                break;

            case "struct":
            case "union":
            case "exception":
                parse_struct(get_struct_field_list(field_info.getJSONObject(field_type_key)),
                        value);
                break;

            case "map":
                parse_map(field_info.getJSONObject(field_type_key), value);
                break;

            case "set":
            case "list":
                parse_list(field_info.getJSONObject(field_type_key), value);
                break;

            default:
                throw new TProtocolException(TProtocolException.INVALID_DATA,
                        new Exception("Unexpected type " + field_type));
        }
    }

    private void parse_map(JSONObject field_info,
                           Object request) throws TProtocolException, JSONException {
        if (!(request instanceof JSONObject)) {
            raise_expected("JSON Object", request);
        }
        JSONObject jsonObject = (JSONObject) request;

        byte key_type = string_to_type_id(field_info.getString(KEY_TYPE_ID_KEY));
        byte value_type = string_to_type_id(field_info.getString(VALUE_TYPE_ID_KEY));
        add(key_type, value_type, jsonObject.length());

        Iterator<String> it = jsonObject.keys();
        while (it.hasNext()) {
            String key = it.next();
            Object value = jsonObject.get(key);
            parse(field_info, key, KEY_TYPE_ID_KEY, KEY_TYPE_KEY);
            parse(field_info, value, VALUE_TYPE_ID_KEY, VALUE_TYPE_KEY);
        }
    }

    private void parse_list(JSONObject field_info,
                            Object request) throws TProtocolException, JSONException {
        if (!(request instanceof JSONArray)) {
            raise_expected("JSON Array", request);
        }
        JSONArray jsonArray = (JSONArray) request;

        byte elem_type = string_to_type_id(field_info.getString(ELEM_TYPE_ID_KEY));
        add(elem_type, jsonArray.length());

        for (int i = 0; i < jsonArray.length(); i++) {
            parse(field_info, jsonArray.get(i), ELEM_TYPE_ID_KEY, ELEM_TYPE_KEY);
        }
    }

    public void parse_struct(JSONArray fields_list,
                             Object request) throws TProtocolException, JSONException {
        if (!(request instanceof JSONObject)) {
            raise_expected("JSON Object", request);
        }
        JSONObject jsonObject = (JSONObject) request;

        Iterator<String> it = jsonObject.keys();
        while (it.hasNext()) {
            String key = it.next();
            Object value = jsonObject.get(key);
            JSONObject field_info = find_in_json_array(fields_list, NAME_KEY, key);
            if (field_info == null) {
                throw new TProtocolException(TProtocolException.INVALID_DATA,
                        new Exception("Unexpected key " + key));
            }

            byte field_type = string_to_type_id(field_info.getString(TYPE_ID_KEY));
            add(key, field_type, field_info.optInt(KEY_KEY, 0));
            parse(field_info, value, TYPE_ID_KEY, TYPE_KEY);
            add("", TType.STOP, -1);
        }
    }

    private byte string_to_type_id(String field_type) throws TProtocolException {
        switch (field_type) {
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
                        new Exception("Unknown type identifier " + field_type));
        }
    }

    @Override
    public TMessage readMessageBegin() throws TException {
        return null;
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
