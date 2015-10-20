package com.devansh.thrift;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.json.JSONArray;

public class HumanReadableJsonProtocolFactory implements TProtocolFactory {

    private JSONArray metadata;
    private String service;

    public HumanReadableJsonProtocolFactory(JSONArray metadata, String service) {
        this.metadata = metadata;
        this.service = service;
    }

    @Override
    public TProtocol getProtocol(TTransport transport) {
        return new HumanReadableJsonProtocol(transport, metadata, service);
    }

}
