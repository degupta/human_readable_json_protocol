package json

import (
	"errors"
	"io"
	"io/ioutil"

	"github.com/wearableintelligence/thrift/lib/go/thrift"
)

const (
	methodKey       = "method"
	servicesKey     = "services"
	nameKey         = "name"
	argumentsKey    = "arguments"
	resultKey       = "result"
	successKey      = "success"
	exceptionsKey   = "exceptions"
	exceptionKey    = "exception"
	functionsKey    = "functions"
	keyKey          = "key"
	onewayKey       = "oneway"
	fieldsKey       = "fields"
	structsKey      = "structs"
	messageKey      = "message"
	classKey        = "class"
	typeIdKey       = "typeId"
	typeKey         = "type"
	keyTypeIdKey    = "keyTypeId"
	keyTypeKey      = "keyType"
	valueTypeIdKey  = "valueTypeId"
	valueTypeKey    = "valueType"
	elemTypeIdKey   = "elemTypeId"
	elemTypeKey     = "elemType"
	returnTypeIdKey = "returnTypeId"
	returnTypeKey   = "returnType"
)

type WIJSONProtocolFactory struct {
	metadata Array
	service  string
}

type WIJSONProtocol struct {
	trans    thrift.TTransport
	oprot    *thrift.TJSONProtocol
	request  Object
	metadata Array
	service  string

	params []interface{}
	err    error
}

func NewWIJSONProtocolFactory(metadata Array, service string) *WIJSONProtocolFactory {
	return &WIJSONProtocolFactory{metadata: metadata, service: service}
}

func (p *WIJSONProtocolFactory) GetProtocol(trans thrift.TTransport) thrift.TProtocol {
	return NewWIJSONProtocol(trans, p.metadata, p.service)
}

func NewWIJSONProtocol(t thrift.TTransport, metadata Array, service string) *WIJSONProtocol {
	return &WIJSONProtocol{
		trans:    t,
		oprot:    thrift.NewTJSONProtocol(t),
		metadata: metadata,
		service:  service,

		params: make([]interface{}, 0),
	}
}

func (protocol *WIJSONProtocol) getMessageTypeAndSeq(methodInfo Object) (thrift.TMessageType, int32) {
	if protocol.request.hasKey(argumentsKey) {
		if methodInfo != nil && methodInfo.getBoolVal(onewayKey) {
			return thrift.ONEWAY, 0
		}
		return thrift.CALL, 0
	} else if protocol.request.hasKey(resultKey) {
		return thrift.REPLY, 1
	} else if protocol.request.hasKey(exceptionKey) {
		return thrift.EXCEPTION, 1
	} else {
		return thrift.INVALID_TMESSAGE_TYPE, 0
	}
}

/**
Basic Format of the request:
{
	"method": "METHOD_NAME",
	"arguments": { ... }
}

Example:
{
	"method": "login",
	"arguments": {
		"email": "user1@wi.co",
		"password": "Pass1234!"
	}
}

The arguments and the names have to match up exactly as specified in the
Thrift definition files.

Basic format of the response
{
	"method": "METHOD_NAME",
	"result|exception": { ... }
}

Example:
{
	"method": "login",
	"result": {
		"success": {
			"authToken": "lsC5dAw1ZUWlMYiK86QTQBdfTaZ9NmXRL28BCt00swIbgkRKIy_zlsiyaCNEHQ8pq36wdF-6TWs9MwtHGQCIUQ==",
			"currentUser": {
				"id": "6a6c982b-62f9-46d2-aff9-bd3a1cdf43f9",
				"email": "user1@wi.co",
				"name": "user1",
				"validatedAt": 0
			},
			"teamIdToTeamRole": {
				"67ec1eb0-3048-4f7a-aec4-8fe4ef99ba89": [
					4,
					3,
					2,
					1
				]
			}
		}
	}
}

If there was an error thrown which was defined in the method definition
{
    "method": "login",
    "result": {
        "err": {
            "errorCode": 401,
            "message": "Invalid email or password"
        }
    }
}

For all other errors:
{
    "method": "loginUser",
    "exception": {
        "message": "Unknown function loginUser",
        "type": 1
    }
}
*/

/**
The parse*() methods parse out the information in the request JSON and add them to the
params array in the EXACT SAME ORDER that the Read*() fields will be called by the
processor on the WIJSONProtocol.

So all we need to do when the Read*() methods are called is to return back how many ever arguments
the method is expected to return from the top of the array
*/

func (protocol *WIJSONProtocol) ReadMessageBegin() (string, thrift.TMessageType, int32, error) {
	// Read all data and parse out the JSON object
	var data []byte
	data, err := ioutil.ReadAll(protocol.trans)
	if err != nil && err != io.EOF && err.Error() != "EOF" { // Uggh thrift wraps it inside its own error
		return "", thrift.INVALID_TMESSAGE_TYPE, 0, thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, err)
	}

	protocol.request, err = NewJson(data)
	if err != nil {
		return "", thrift.INVALID_TMESSAGE_TYPE, 0, thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, err)
	}

	// Get the method info corresponding to method being called
	name := protocol.request.getDefaultStringVal(methodKey, "")
	methodInfo, methodInfoErr := protocol.getMethodInfo(protocol.service, name)
	// Type Id tells whether it is a request (CALL) or response (REPLY) or error (EXCEPTION)
	// Seq Id == 0 for CALL and 1 for REPLY, EXCEPTION always
	typeId, seqId := protocol.getMessageTypeAndSeq(methodInfo)

	if protocol.request.hasKey(argumentsKey) {
		/** CALL */

		if methodInfoErr != nil {
			// Method not found, STOP processing and send back up,
			// Generated code will send an error saying "Unknown function"
			protocol.add("", thrift.TType(thrift.STOP), int16(-1))
			return name, typeId, seqId, nil
		}

		// Parse out the arguments struct
		// Send it the arguments struct in both the method metadata and the request
		protocol.err = protocol.parseStruct(methodInfo.getArrayVal(argumentsKey), protocol.request.getJsonVal(argumentsKey))
	} else if protocol.request.hasKey(resultKey) {
		/** REPLY */

		if methodInfoErr != nil {
			// Method not found, STOP processing and send back up,
			// Generated code will send an error saying "Unknown function"
			protocol.add("", thrift.TType(thrift.STOP), int16(-1))
			return name, typeId, seqId, nil
		}

		result := protocol.request.getJsonVal(resultKey)

		// The result will either have "success" or the name of the error that was thrown
		if result.hasKey(successKey) {
			// Figure out the return type
			returnType, err := protocol.StringToTypeId(methodInfo.getStringVal(returnTypeIdKey))
			if err != nil {
				protocol.err = err
			} else {
				// Add a field with the returnType
				protocol.add("", returnType, int16(0))
				// Parse the result
				protocol.err = protocol.parse(methodInfo, result[successKey], returnTypeIdKey, returnTypeKey)
				// Finish parsing
				protocol.add("", thrift.TType(thrift.STOP), int16(-1))
			}
		} else if len(result) == 0 {
			// There was nothing in result. The return type is void
			protocol.add("", thrift.TType(thrift.STOP), int16(-1))
		} else {
			// There was an error, get the error variable name
			var errName string
			for key, _ := range result {
				errName = key
				break
			}

			errInfo := methodInfo.getArrayVal(exceptionsKey).findInJsonArray(nameKey, errName)
			if errInfo == nil {
				// Error doesn't exist in the Thrift definition
				return "", thrift.INVALID_TMESSAGE_TYPE, 0, thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, errors.New("Unable to parse result"))
			}
			// Begin Parsing Error (which is just a struct)
			protocol.add(errName, thrift.TType(thrift.STRUCT), errInfo.getDefaultInt16Val(keyKey, 1))
			// Parse Error
			protocol.err = protocol.parse(errInfo, result.getJsonVal(errName), typeIdKey, typeKey)
			// Finish Parsing error
			protocol.add("", thrift.TType(thrift.STOP), int16(-1))
		}

	} else if protocol.request.hasKey(exceptionKey) {
		// Error other than one defined in the method signature. It will have a string and an int32
		// Add String field
		protocol.add("", thrift.TType(thrift.STRING), int16(1))
		// Parse string
		protocol.add(protocol.request.getJsonVal(exceptionKey).getDefaultStringVal(messageKey, ""))
		// Add int32
		protocol.add("", thrift.TType(thrift.I32), int16(2))
		// Parse int32
		protocol.add(protocol.request.getJsonVal(exceptionKey).getDefaultInt32Val(typeKey, thrift.UNKNOWN_APPLICATION_EXCEPTION))
		// Finish Parsing
		protocol.add("", thrift.TType(thrift.STOP), int16(-1))

	} else {
		// Don't recognize what kind of message this is
		return "", thrift.INVALID_TMESSAGE_TYPE, 0, thrift.NewTProtocolExceptionWithType(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, errors.New("Invalid type"))
	}

	// fmt.Println(protocol.params)

	return name, typeId, seqId, nil
}

func (protocol *WIJSONProtocol) ReadMessageEnd() error {
	return nil
}

func (protocol *WIJSONProtocol) ReadStructBegin() (string, error) {
	// fmt.Println("ReadStructBegin")
	return "", protocol.err
}

func (protocol *WIJSONProtocol) ReadStructEnd() error {
	// fmt.Println("ReadStructEnd")
	return nil
}

func (protocol *WIJSONProtocol) ReadFieldBegin() (string, thrift.TType, int16, error) {
	p := protocol.slice(3)
	// fmt.Println("ReadFieldBegin", p)
	return p[0].(string), p[1].(thrift.TType), p[2].(int16), nil
}

func (protocol *WIJSONProtocol) ReadFieldEnd() error {
	// fmt.Println("ReadFieldEnd")
	return nil
}

func (protocol *WIJSONProtocol) ReadMapBegin() (thrift.TType, thrift.TType, int, error) {
	p := protocol.slice(3)
	// fmt.Println("ReadMapBegin", p)
	return p[0].(thrift.TType), p[1].(thrift.TType), p[2].(int), nil
}

func (protocol *WIJSONProtocol) ReadMapEnd() error {
	// fmt.Println("ReadMapEnd")
	return nil
}

func (protocol *WIJSONProtocol) ReadListBegin() (thrift.TType, int, error) {
	p := protocol.slice(2)
	// fmt.Println("ReadListBegin", p)
	return p[0].(thrift.TType), p[1].(int), nil
}

func (protocol *WIJSONProtocol) ReadListEnd() error {
	// fmt.Println("ReadListEnd")
	return nil
}

func (protocol *WIJSONProtocol) ReadSetBegin() (elemType thrift.TType, size int, err error) {
	p := protocol.slice(2)
	// fmt.Println("ReadSetBegin")
	return p[0].(thrift.TType), p[1].(int), nil
}

func (protocol *WIJSONProtocol) ReadSetEnd() error {
	// fmt.Println("ReadSetEnd")
	return nil
}

func (protocol *WIJSONProtocol) ReadBool() (bool, error) {
	p := protocol.slice(1)
	// fmt.Println("ReadBool", p)
	return p[0].(bool), nil
}

func (protocol *WIJSONProtocol) ReadByte() (byte, error) {
	p := protocol.slice(1)
	// fmt.Println("ReadByte", p)
	return p[0].(byte), nil
}

func (protocol *WIJSONProtocol) ReadI16() (int16, error) {
	p := protocol.slice(1)
	// fmt.Println("ReadI16", p)
	return p[0].(int16), nil
}

func (protocol *WIJSONProtocol) ReadI32() (int32, error) {
	p := protocol.slice(1)
	// fmt.Println("ReadI32", p)
	return p[0].(int32), nil
}

func (protocol *WIJSONProtocol) ReadI64() (int64, error) {
	p := protocol.slice(1)
	// fmt.Println("ReadI64", p)
	return p[0].(int64), nil
}

func (protocol *WIJSONProtocol) ReadDouble() (float64, error) {
	p := protocol.slice(1)
	// fmt.Println("ReadDouble", p)
	return p[0].(float64), nil
}

func (protocol *WIJSONProtocol) ReadString() (string, error) {
	p := protocol.slice(1)
	// fmt.Println("ReadString", p)
	return p[0].(string), nil
}

func (protocol *WIJSONProtocol) ReadBinary() ([]byte, error) {
	// fmt.Println("ReadBinary")
	v, err := protocol.ReadString()
	if err != nil {
		return nil, err
	} else {
		return []byte(v), err
	}
}

func (protocol *WIJSONProtocol) Flush() (err error) {
	return protocol.oprot.Flush()
}

func (protocol *WIJSONProtocol) Skip(fieldType thrift.TType) (err error) {
	return thrift.SkipDefaultDepth(protocol, fieldType)
}

func (protocol *WIJSONProtocol) Transport() thrift.TTransport {
	return protocol.trans
}

func (protocol *WIJSONProtocol) WriteMessageBegin(name string, typeId thrift.TMessageType, seqid int32) error {
	err := protocol.oprot.OutputObjectBegin()
	if err != nil {
		return err
	}
	err = protocol.oprot.WriteString(methodKey)
	if err != nil {
		return err
	}
	err = protocol.oprot.WriteString(name)
	if err != nil {
		return err
	}

	switch typeId {

	case thrift.CALL:
		err = protocol.oprot.WriteString(argumentsKey)
		if err != nil {
			return err
		}

	case thrift.REPLY:
		err = protocol.oprot.WriteString(resultKey)
		if err != nil {
			return err
		}

	case thrift.EXCEPTION:
		err = protocol.oprot.WriteString(exceptionKey)
		if err != nil {
			return err
		}
	}
	return nil
}

// Simply use the simple json protocol to write out the
// JSON
func (protocol *WIJSONProtocol) WriteMessageEnd() error {
	return protocol.oprot.OutputObjectEnd()
}

func (protocol *WIJSONProtocol) WriteStructBegin(name string) error {
	return protocol.oprot.OutputObjectBegin()
}

func (protocol *WIJSONProtocol) WriteStructEnd() error {
	return protocol.oprot.OutputObjectEnd()
}

func (protocol *WIJSONProtocol) WriteFieldBegin(name string, typeId thrift.TType, id int16) error {
	return protocol.oprot.WriteString(name)
}

func (protocol *WIJSONProtocol) WriteFieldEnd() error {
	return nil
}

func (protocol *WIJSONProtocol) WriteFieldStop() error {
	return nil
}

func (protocol *WIJSONProtocol) WriteMapBegin(keyType thrift.TType, valueType thrift.TType, size int) error {
	return protocol.oprot.OutputObjectBegin()
}

func (protocol *WIJSONProtocol) WriteMapEnd() error {
	return protocol.oprot.OutputObjectEnd()
}

func (protocol *WIJSONProtocol) WriteListBegin(elemType thrift.TType, size int) error {
	return protocol.oprot.OutputListBegin()
}

func (protocol *WIJSONProtocol) WriteListEnd() error {
	return protocol.oprot.OutputListEnd()
}

func (protocol *WIJSONProtocol) WriteSetBegin(elemType thrift.TType, size int) error {
	return protocol.oprot.OutputListBegin()
}

func (protocol *WIJSONProtocol) WriteSetEnd() error {
	return protocol.oprot.OutputListEnd()
}

func (protocol *WIJSONProtocol) WriteBool(value bool) error {
	return protocol.oprot.OutputBool(value)
}

func (protocol *WIJSONProtocol) WriteByte(value byte) error {
	return protocol.oprot.WriteByte(value)
}

func (protocol *WIJSONProtocol) WriteI16(value int16) error {
	return protocol.oprot.WriteI16(value)
}

func (protocol *WIJSONProtocol) WriteI32(value int32) error {
	return protocol.oprot.WriteI32(value)
}

func (protocol *WIJSONProtocol) WriteI64(value int64) error {
	return protocol.oprot.WriteI64(value)
}

func (protocol *WIJSONProtocol) WriteDouble(value float64) error {
	return protocol.oprot.WriteDouble(value)
}

func (protocol *WIJSONProtocol) WriteString(value string) error {
	return protocol.oprot.WriteString(value)
}

func (protocol *WIJSONProtocol) WriteBinary(value []byte) error {
	return protocol.oprot.WriteBinary(value)
}
