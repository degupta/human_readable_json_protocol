package com.devansh.humanthrift;

import com.devansh.humanthrift.generated.AuthenticationService;
import com.devansh.humanthrift.generated.LoginResult;
import com.devansh.humanthrift.generated.SystemException;
import com.devansh.humanthrift.generated.User;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.json.JSONArray;

public class Main {

    public static void main(String[] args) throws Exception {
        // First Read in all the Metadata
        JSONArray jsonMetadata = HumanReadableJsonHelpers.readAllFiles("src/main/resources/thrift-json");

        // Next create the Factory, pass in the exact name - AuthenticationService
        HumanReadableJsonProtocol.Factory factory = new HumanReadableJsonProtocol.Factory(jsonMetadata,
                "AuthenticationService");

        // Using a Memory Buffer to show how the request is serialized, but you can use any transport you want
        TMemoryBuffer memoryBuffer = new TMemoryBuffer(1024);
        AuthenticationService.Client client = new AuthenticationService.Client(factory.getProtocol(memoryBuffer));
        try {
            client.login("devansh@devash.com", "p@$$w0rd");
        } catch (Exception e) {
            // Ignore error, its just saying there is no response (since we don't have a server)
        }

        String request = memoryBuffer.toString("UTF-8");
        System.out.println("Request:");
        System.out.println(request);
        System.out.println();

        TMemoryBuffer memoryBufferResp = new TMemoryBuffer(1024);
        getProcessor().process(factory.getProtocol(new TMemoryInputTransport(request.getBytes())),
                factory.getProtocol(memoryBufferResp));

        String response = memoryBufferResp.toString("UTF-8");
        System.out.println("Response:");
        System.out.println(response);
        System.out.println();
    }

    private static AuthenticationService.Processor getProcessor() {
        return new AuthenticationService.Processor<>(new AuthenticationService.Iface() {
            @Override
            public LoginResult login(String email, String password) throws SystemException, TException {
                if (email.equals("devansh@devash.com") && password.equals("p@$$w0rd")) {
                    return new LoginResult()
                            .setAuthToken("ABCDE")
                            .setCurrentUser(new User()
                                    .setId("1")
                                    .setEmail("devansh@devash.com")
                                    .setName("Devansh Gupta")
                                    .setValidatedAt(System.currentTimeMillis() / 1000));
                } else {
                    throw new SystemException(401, "Unknown User");
                }
            }
        });
    }
}
