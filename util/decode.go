package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	log "github.com/sirupsen/logrus"
)

func decodeType(r io.Reader, v reflect.Value) (err error) {
	switch value := v.Interface().(type) {
	case bool:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetBool(value)
	case int8:
		err = binary.Read(r, binary.BigEndian, &value)
		v.SetInt(int64(value))
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
		case reflect.Struct:
			err = decodeStruct(r, v)
		}
	}

	// v.Set(reflect.ValueOf(value))
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
	for i := 0; i < v.Type().NumField(); i++ {
		err = decodeType(r, v.Field(i))
		if err != nil {
			return err
		}
	}
	return nil
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
