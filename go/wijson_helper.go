package json

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wearableintelligence/thrift/lib/go/thrift"
)

func (protocol *WIJSONProtocol) getMethodInfo(serviceName, methodName string) (Object, error) {
	for _, v := range protocol.metadata {
		// For each package in the metadata
		pkg, ok := toJsonCheck(v)
		if !ok {
			continue
		}
		// check if it has any services
		if svcs, err := pkg.getArrayValSafe(servicesKey); err == nil {
			// check if it has the service we are looking for
			svc := svcs.findInJsonArray(nameKey, pkg.getStringVal(nameKey)+"."+serviceName)
			if svc != nil {
				// check if it has the method we are looking for
				methodInfo := svc.getArrayVal(functionsKey).findInJsonArray(nameKey, methodName)
				if methodInfo != nil {
					return methodInfo, nil
				}
			}
		}
	}

	// Did not find the method, return error
	return nil, errors.New(serviceName + "::" + methodName + " not found!")
}

func (protocol *WIJSONProtocol) getInfo(class string) Object {
	parts := strings.Split(class, ".")
	// Find the package we are looking for
	program := protocol.metadata.findInJsonArray(nameKey, parts[0])
	// In the structs array, check if the struct we are looking for exists
	return program.getArrayVal(structsKey).findInJsonArray(nameKey, parts[1])
}

func (protocol *WIJSONProtocol) getStructFieldList(elemType Object) Array {
	// Get the struct we are looking for and then get the list of fields in the struct
	return protocol.getInfo(elemType.getStringVal(classKey)).getArrayVal(fieldsKey)
}

/**
The parse*() methods parse out the information in the request JSON and add them to the
params array in the EXACT SAME ORDER that the Read*() fields will be called by the
processor on the WIJSONProtocol.

So all we need to do when the Read*() methods are called is to return back how many ever arguments
the method is expected to return from the top of the array
*/

func (protocol *WIJSONProtocol) parse(fieldInfo Object, value interface{}, fieldTypeIdKey, fieldTypeKey string) error {
	// Get the type of the value we expect
	// Check if value is of that type
	// and then add it to the list of arguments
	fieldType := fieldInfo.getStringVal(fieldTypeIdKey)

	switch fieldType {
	case "bool":
		// fmt.Println("ReadBool")
		if v, ok := value.(bool); ok {
			protocol.add(v)
			return nil
		} else {
			return errors.New("Expected bool")
		}

	case "i8":
		// fmt.Println("ReadByte")
		if v, ok := value.(float64); ok {
			protocol.add(byte(v))
			return nil
		} else {
			return errors.New("Expected byte")
		}

	case "i16":
		// fmt.Println("ReadI16")
		if v, ok := value.(float64); ok {
			protocol.add(int16(v))
			return nil
		} else {
			return errors.New("Expected int16")
		}

	case "i32":
		// fmt.Println("ReadI32")
		if v, ok := value.(float64); ok {
			protocol.add(int32(v))
			return nil
		} else {
			return errors.New("Expected int32")
		}

	case "i64":
		// fmt.Println("ReadI64")
		if v, ok := value.(float64); ok {
			protocol.add(int64(v))
			return nil
		} else {
			return errors.New("Expected int64")
		}

	case "double":
		// fmt.Println("ReadDouble")
		if v, ok := value.(float64); ok {
			protocol.add(v)
			return nil
		} else {
			return errors.New("Expected double")
		}

	case "string":
		// fmt.Println("ReadString")
		if v, ok := value.(string); ok {
			protocol.add(v)
			return nil
		} else {
			return errors.New("Expected string")
		}

	case "struct", "union", "exception":
		return protocol.parseStruct(protocol.getStructFieldList(fieldInfo.getJsonVal(fieldTypeKey)), value)

	case "map":
		return protocol.parseMap(fieldInfo.getJsonVal(fieldTypeKey), value)

	case "set", "list":
		return protocol.parseList(fieldInfo.getJsonVal(fieldTypeKey), value)
	}

	return errors.New("Unexpected type " + fieldType)
}

func (protocol *WIJSONProtocol) parseMap(fieldInfo Object, request interface{}) error {
	if object, ok := toJsonCheck(request); ok {
		// fmt.Println("ReadMapBegin")

		// Get the key type
		keyType, err := protocol.StringToTypeId(fieldInfo.getStringVal(keyTypeIdKey))
		if err != nil {
			return err
		}

		// Get the value type
		valueType, err := protocol.StringToTypeId(fieldInfo.getStringVal(valueTypeIdKey))
		if err != nil {
			return err
		}

		// Add them to list of arguments
		protocol.add(keyType, valueType, len(object))

		// For each key, value in the map recurse
		for key, value := range object {
			// Parse key
			err = protocol.parse(fieldInfo, key, keyTypeIdKey, keyTypeKey)
			if err != nil {
				return err
			}

			// Parse value
			err = protocol.parse(fieldInfo, value, valueTypeIdKey, valueTypeKey)
			if err != nil {
				return err
			}
		}
		// fmt.Println("ReadMapEnd")
		return nil
	} else {
		return errors.New("Expected Map (Json Object)")
	}
}

func (protocol *WIJSONProtocol) parseList(fieldInfo Object, request interface{}) error {
	if array, ok := ToJsonArrayCheck(request); ok {
		// fmt.Println("ReadListBegin/ReadSetBegin")

		// Get the element type of the list
		elemType, err := protocol.StringToTypeId(fieldInfo.getStringVal(elemTypeIdKey))
		if err != nil {
			return err
		}

		// Add the element type and length to the arguments
		protocol.add(elemType, len(array))

		// Recursively parse each element in the array
		for _, value := range array {
			err = protocol.parse(fieldInfo, value, elemTypeIdKey, elemTypeKey)
			if err != nil {
				return err
			}
		}
		// fmt.Println("ReadListEnd/ReadSetEnd")
		return nil
	} else {
		return errors.New("Expected Json Array")
	}
}

func (protocol *WIJSONProtocol) parseStruct(fieldsList Array, request interface{}) error {
	if object, ok := toJsonCheck(request); ok {
		// fmt.Println("ReadStructBegin", object)
		for key, value := range object {
			// fmt.Println("ReadFieldBegin")

			// Get the information of the field (type, name, etc)
			fieldInfo := fieldsList.findInJsonArray(nameKey, key)
			if fieldInfo == nil {
				// Field doesn't exist
				return errors.New("Unexpected key " + key)
			}

			// Get the Thrift Type of the field
			fieldType, err := protocol.StringToTypeId(fieldInfo.getStringVal(typeIdKey))
			if err != nil {
				return err
			}

			// Add the name, type and key (1:, 2:, 3:, etc) to params
			protocol.add(key, fieldType, fieldInfo.getDefaultInt16Val(keyKey, 0))

			// fmt.Println(key, fieldType, fieldInfo.GetInt16Val(keyKey))

			// Recursively parse the field
			err = protocol.parse(fieldInfo, value, typeIdKey, typeKey)
			if err != nil {
				return err
			}
			// fmt.Println("ReadFieldEnd")
		}

		// Finish parsing struct
		protocol.add("", thrift.TType(thrift.STOP), int16(-1))
		// fmt.Println("ReadStructEnd")
		return nil
	} else {
		return errors.New(fmt.Sprintf("Expected Json Object got %t", request))
	}
}

func (protocol *WIJSONProtocol) add(elem ...interface{}) {
	protocol.params = append(protocol.params, elem...)
}

func (protocol *WIJSONProtocol) slice(num int) []interface{} {
	p := protocol.params[0:num]
	protocol.params = protocol.params[num:len(protocol.params)]
	return p
}

func (protocol *WIJSONProtocol) StringToTypeId(fieldType string) (thrift.TType, error) {
	switch fieldType {
	case "bool":
		return thrift.TType(thrift.BOOL), nil
	case "i8":
		return thrift.TType(thrift.BYTE), nil
	case "i16":
		return thrift.TType(thrift.I16), nil
	case "i32":
		return thrift.TType(thrift.I32), nil
	case "i64":
		return thrift.TType(thrift.I64), nil
	case "double":
		return thrift.TType(thrift.DOUBLE), nil
	case "string":
		return thrift.TType(thrift.STRING), nil
	case "struct", "union", "exception":
		return thrift.TType(thrift.STRUCT), nil
	case "map":
		return thrift.TType(thrift.MAP), nil
	case "set":
		return thrift.TType(thrift.SET), nil
	case "list":
		return thrift.TType(thrift.LIST), nil
	}

	e := fmt.Errorf("Unknown type identifier: %s", fieldType)
	return thrift.TType(thrift.STOP), thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, e)
}
