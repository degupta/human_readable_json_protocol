package json

import (
	"encoding/json"
	"errors"
)

var (
	errInvalidKey  = errors.New("Invalid Key")
	errInvalidJson = errors.New("Invalid JSON data")
)

type Object map[string]interface{}
type Array []interface{}

func NewJson(data []byte) (Object, error) {
	var m interface{}
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	if json, ok := m.(map[string]interface{}); ok {
		return Object(json), nil
	} else {
		return nil, errInvalidJson
	}
}

func toJson(val interface{}) Object {
	return Object(val.(map[string]interface{}))
}

func toJsonCheck(val interface{}) (Object, bool) {
	switch vx := val.(type) {
	case Object:
		return vx, true
	case map[string]interface{}:
		return Object(vx), true
	}
	return nil, false
}

func ToJsonArrayCheck(val interface{}) (Array, bool) {
	switch vx := val.(type) {
	case Array:
		return vx, true
	case []interface{}:
		return Array(vx), true
	}
	return nil, false
}

func (json Object) getVal(keys ...string) interface{} {
	current := json
	l := len(keys) - 1
	for i := 0; i < l; i++ {
		current = toJson(current[keys[i]])
	}
	return current[keys[l]]
}

func (json Object) getDefaultInt16Val(key string, defaultVal int16) int16 {
	if val, ok := json[key]; ok {
		return int16(val.(float64))
	} else {
		return defaultVal
	}
}

func (json Object) getDefaultInt32Val(key string, defaultVal int32) int32 {
	if val, ok := json[key]; ok {
		return int32(val.(float64))
	} else {
		return defaultVal
	}
}

func (json Object) getStringVal(keys ...string) string {
	return json.getVal(keys...).(string)
}

func (json Object) getArrayVal(keys ...string) Array {
	return Array(json.getVal(keys...).([]interface{}))
}

func (json Object) getJsonVal(keys ...string) Object {
	return toJson(json.getVal(keys...))
}

func (arr Array) findInJsonArray(key string, val string) Object {
	for _, v := range arr {
		switch vx := v.(type) {
		case Object:
			if vx[key] == val {
				return vx
			}
		case map[string]interface{}:
			if vx[key] == val {
				return Object(vx)
			}
		}
	}
	return nil
}

func (json Object) getBoolVal(keys ...string) bool {
	return json.getVal(keys...).(bool)
}

func (json Object) getDefaultStringVal(key, defaultVal string) string {
	if val, ok := json[key]; ok {
		return val.(string)
	} else {
		return defaultVal
	}
}

func (json Object) hasKey(key string) bool {
	_, ok := json[key]
	return ok
}

func (json Object) getArrayValSafe(keys ...string) (Array, error) {
	if val, ok := json.getVal(keys...).([]interface{}); ok {
		return Array(val), nil
	} else {
		return nil, errInvalidKey
	}
}
