# Human Readable Json Protocol
A way to convert Thrift Services and Functions into Human Readable JSON

Basic Format of the request:
```json
{
  "method": "METHOD_NAME",
  "arguments": { ... }
}
```

Example:
```json
{
  "method": "login",
  "arguments": {
    "email": "devansh@wi.co",
    "password": "password"
  }
}
```

The arguments and the names have to match up exactly as specified in the
Thrift definition files.

Basic format of the response
```json
{
  "method": "METHOD_NAME",
  "result|exception": { ... }
}
```

Example:
```json
{
  "method": "login",
  "result": {
    "success": {
      "authToken": "some_auth_token",
      "currentUser": {
        "id": "6a6c982b-62f9-46d2-aff9-bd3a1cdf43f9",
        "email": "user1@wi.co",
        "name": "user1",
        "validatedAt": 0
      }
    }
  }
}
```

If there was an error thrown which was defined in the method definition
```json
{
    "method": "login",
    "result": {
        "err": {
            "errorCode": 401,
            "message": "Invalid email or password"
        }
    }
}
```

For all other errors:
```json
{
    "method": "loginUser",
    "exception": {
        "message": "Unknown function loginUser",
        "type": 1
    }
}
```
