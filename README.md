# Human Readable Json Protocol
A way to convert Thrift Services and Functions into Human Readable JSON

For a simple Thrift service like so:

```
exception SystemException {
  1: i32 errorCode,
  2: string message,
}

struct User {
  1: string id,
  2: string email,
  3: string name,
  4: i64 validatedAt
}

struct LoginResult {
  1: string authToken,
  2: User currentUser
}

service AuthenticationService {
  LoginResult login(1: string email, 2: string password) throws(1: SystemException err)
}
```

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

**_The arguments and the names have to match up exactly as specified in the Thrift definition files._**

Basic format of the response
```json
{
  "method": "METHOD_NAME",
  "result|exception": { ... }
}
```
**_Note that the JSON objects have keys whose names are the same as that of fields in the struct defined in Thrift_**

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


**_If there was an error thrown which was defined in the method definition_**
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


### Modifying the Thrift source code
You will need to make a minor change to the Thrift JSON metadata generator in order for this to work. You can look at the diff of the change that is required over here: [JSON Generator Diff](diff_for_t_json_generator_cc.diff).

All this change does it to make sure when the class type is written out it displays as *package.class_name* instead of simply *class_name*.
