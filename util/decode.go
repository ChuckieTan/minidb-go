package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"minidb-go/parser/ast"
	"reflect"

	log "github.com/sirupsen/logrus"
)

func decodeType(r io.Reader, v reflect.Value) (err error) {
	switch value := v.Interface().(type) {
	case ast.ColumnType:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetUint(uint64(ast.ColumnType(value)))
	case bool:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetBool(value)
	case int8:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetInt(int64(value))
	case int:
		newValue := int32(0)
		err = binary.Read(r, binary.BigEndian, &newValue)
		v.SetInt(int64(newValue))
	case int32:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetInt(int64(value))
	case int64:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetInt(int64(value))
	case uint8:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetUint(uint64(value))
	case uint32:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetUint(uint64(value))
	case uint64:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetUint(uint64(value))
	case float32:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetFloat(float64(value))
	case float64:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetFloat(float64(value))

	default:
		switch v.Kind() {
		case reflect.String:
			err = decodeString(r, v)
		case reflect.Array:
			err = decodeArray(r, v)
		case reflect.Slice:
			err = decodeSlice(r, v)
		case reflect.Map:
			err = decodeMap(r, v)
		case reflect.Struct:
			err = decodeStruct(r, v)
		case reflect.Ptr:
			err = decodeType(r, v.Elem())
		case reflect.Interface:
			if v.Addr().CanConvert(reflect.TypeOf((*ast.SQLExprValue)(nil))) {
				err = decodeSQLExprValue(r, v)
			} else {
				err = fmt.Errorf("decode: unsupported type: %T", v.Addr().Interface())
				log.Error(err.Error())
				return
			}
		default:
			err = fmt.Errorf("decode: unsupported type: %T", v.Addr().Interface())
			log.Error(err.Error())
			return
		}
	}
	return
}

func decodeMap(r io.Reader, v reflect.Value) (err error) {
	var l int32
	binary.Read(r, binary.BigEndian, &l)

	newMap := reflect.MakeMapWithSize(
		reflect.MapOf(v.Type().Key(), v.Type().Elem()), int(l))
	for i := int32(0); i < l; i++ {
		key := reflect.New(v.Type().Key())
		err = decodeType(r, key.Elem())
		if err != nil {
			return
		}
		value := reflect.New(v.Type().Elem())
		err = decodeType(r, value.Elem())
		if err != nil {
			return
		}
		newMap.SetMapIndex(key.Elem(), value.Elem())
	}
	v.Set(newMap)
	return
}

func decodeString(r io.Reader, v reflect.Value) (err error) {
	var l int32
	binary.Read(r, binary.BigEndian, &l)
	bytes := make([]byte, l)
	_, err = r.Read(bytes)
	v.SetString(string(bytes))
	return
}

func decodeArray(r io.Reader, v reflect.Value) (err error) {
	for i := 0; i < v.Len(); i++ {
		err = decodeType(r, v.Index(i))
		if err != nil {
			return
		}
	}
	return
}

func decodeSlice(r io.Reader, v reflect.Value) (err error) {
	var l int32
	binary.Read(r, binary.BigEndian, &l)
	// 新建一个指定容量的 slice
	slice := reflect.MakeSlice(v.Type(), int(l), int(l))
	for i := 0; i < int(l); i++ {
		err = decodeType(r, slice.Index(i))
		if err != nil {
			return
		}
	}
	v.Set(slice)
	return
}

func decodeStruct(r io.Reader, v reflect.Value) (err error) {
	fieldType := v.Type()
	for i := 0; i < v.Type().NumField(); i++ {
		if fieldType.Field(i).Tag.Get("encode") == "false" {
			continue
		}
		err = decodeType(r, v.Field(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeSQLExprValue(r io.Reader, v reflect.Value) (err error) {
	typeNumber := uint8(9)
	err = binary.Read(r, binary.BigEndian, &typeNumber)
	if err != nil {
		return
	}
	switch typeNumber {
	case 0:
		num := int64(0)
		err = binary.Read(r, binary.BigEndian, &num)
		newSQLValue := reflect.New(reflect.TypeOf(ast.SQLInt(0)))
		newSQLValue.Elem().SetInt(num)
		v.Set(newSQLValue.Elem())
	case 1:
		num := float64(0)
		err = binary.Read(r, binary.BigEndian, &num)
		newSQLValue := reflect.New(reflect.TypeOf(ast.SQLFloat(0)))
		newSQLValue.Elem().SetFloat(num)
		v.Set(newSQLValue.Elem())
	case 2, 3:
		num := ""
		err = decodeType(r, reflect.ValueOf(&num))
		newSQLValue := reflect.New(reflect.TypeOf(ast.SQLText("")))
		newSQLValue.Elem().SetString(num)
		v.Set(newSQLValue.Elem())
	}
	return
}

func Decode(r io.Reader, origin interface{}) (err error) {
	value := reflect.ValueOf(origin)
	if value.Kind() != reflect.Ptr {
		err = fmt.Errorf("attempt decode to a non ptr value")
		log.Error(err.Error())
		return
	}
	err = decodeType(r, value.Elem())
	return
}
