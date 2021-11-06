package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"minidb-go/parser/ast"
	"reflect"

	log "github.com/sirupsen/logrus"
)

func encodeType(w io.Writer, v reflect.Value) (err error) {
	// 转换 SQL 数据类型
	// 0 INT, 1 FLOAT, 2 STRING
	if v.Kind() == reflect.Interface {
		switch v.Interface().(type) {
		case ast.SQLInt:
			err = binary.Write(w, binary.BigEndian, int8(0))
		case ast.SQLFloat:
			// v = float64(exprValue)
			err = binary.Write(w, binary.BigEndian, int8(1))
		case ast.SQLText:
			// v = string(exprValue)
			err = binary.Write(w, binary.BigEndian, int8(2))
		case ast.SQLColumn:
			err = binary.Write(w, binary.BigEndian, int8(3))
		}
	}

	if v.Kind() != reflect.Slice {
		// 尝试写入定长数据
		werr := binary.Write(w, binary.BigEndian, v.Interface())
		if werr == nil {
			return
		}
	}

	switch fieldType := v.Kind(); fieldType {
	// int 不是定长，转换为 int32
	case reflect.Int:
		err = binary.Write(w, binary.BigEndian, int32(v.Int()))

	// 指针不进行序列化
	case reflect.Ptr:
		return

	case reflect.String:
		err = encodeString(w, v.String())

	case reflect.Slice:
		err = encodeSlice(w, v)

	case reflect.Array:
		err = encodeArray(w, v)

	case reflect.Map:
		err = encodeMap(w, v)

	case reflect.Struct:
		err = encodeStruct(w, v)

	case reflect.Interface:
		err = encodeType(w, v.Elem())

	default:
		err = fmt.Errorf("encode: unsupported type: %v", fieldType)
		log.Error(err.Error())
		return
	}
	return
}

func encodeString(w io.Writer, str string) (err error) {
	// 先写字符串长度，再写数据
	encodeType(w, reflect.ValueOf(len(str)))
	w.Write([]byte(str))
	return nil
}

func encodeArray(w io.Writer, v reflect.Value) (err error) {
	for i := 0; i < v.Len(); i++ {
		err = encodeType(w, v.Index(i))
		if err != nil {
			return
		}
	}
	return
}

func encodeSlice(w io.Writer, v reflect.Value) (err error) {
	// 写入 slice 长度
	encodeType(w, reflect.ValueOf(v.Len()))
	for i := 0; i < v.Len(); i++ {
		err = encodeType(w, v.Index(i))
		if err != nil {
			return
		}
	}
	return
}

func encodeMap(w io.Writer, v reflect.Value) (err error) {
	for _, key := range v.MapKeys() {
		err = encodeType(w, key)
		if err != nil {
			return
		}
		err = encodeType(w, v.MapIndex(key))
		if err != nil {
			return
		}
	}
	return
}

func encodeStruct(w io.Writer, v reflect.Value) (err error) {
	for i := 0; i < v.NumField(); i++ {
		err = encodeType(w, v.Field(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func Encode(w io.Writer, v interface{}) (err error) {
	err = encodeType(w, reflect.ValueOf(v))
	return err
}
