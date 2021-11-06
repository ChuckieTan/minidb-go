package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"minidb-go/parser/ast"
	"minidb-go/parser/token"
	"reflect"

	log "github.com/sirupsen/logrus"
)

func encodeType(buffer io.Writer, origin interface{}) (err error) {
	if reflect.TypeOf(origin).Kind() != reflect.Slice {
		// 尝试写入定长数据
		werr := binary.Write(buffer, binary.BigEndian, origin)
		if werr == nil {
			return
		}
	}

	// 转换 SQL 数据类型
	// 0 INT, 1 FLOAT, 2 STRING
	switch exprValue := origin.(type) {
	case ast.SQLInt:
		origin = int64(exprValue)
		err = binary.Write(buffer, binary.BigEndian, int8(0))
	case ast.SQLFloat:
		origin = float64(exprValue)
		err = binary.Write(buffer, binary.BigEndian, int8(1))
	case ast.SQLText:
		origin = string(exprValue)
		err = binary.Write(buffer, binary.BigEndian, int8(2))
	case ast.SQLColumn:
		origin = string(exprValue)
	case token.TokenType:
		origin = int(exprValue)
	}

	switch fieldType := reflect.TypeOf(origin).Kind(); fieldType {
	// int 不是定长，转换为 int32
	case reflect.Int:
		err = binary.Write(buffer, binary.BigEndian, int32(origin.(int)))

	// 指针不进行序列化
	case reflect.Ptr:
		return

	case reflect.String:
		err = encodeString(buffer, origin.(string))

	case reflect.Slice:
		err = encodeSlice(buffer, origin)

	case reflect.Array:
		err = encodeArray(buffer, origin)

	case reflect.Struct:
		err = encodeStruct(buffer, origin)

	default:
		err = fmt.Errorf("encode: unsupported type: %v", fieldType)
		log.Error(err.Error())
		return
	}
	return
}

func encodeString(buffer io.Writer, str string) (err error) {
	// 先写字符串长度，再写数据
	encodeType(buffer, len(str))
	buffer.Write([]byte(str))
	return nil
}

func encodeSlice(buffer io.Writer, origin interface{}) (err error) {
	value := reflect.ValueOf(origin)

	// 写入 slice 长度
	encodeType(buffer, value.Len())
	for i := 0; i < value.Len(); i++ {
		err = encodeType(buffer, value.Index(i).Interface())
		if err != nil {
			return
		}
	}
	return
}

func encodeArray(buffer io.Writer, origin interface{}) (err error) {
	value := reflect.ValueOf(origin)

	for i := 0; i < value.Len(); i++ {
		err = encodeType(buffer, value.Index(i).Interface())
		if err != nil {
			return
		}
	}
	return
}

func encodeStruct(buffer io.Writer, origin interface{}) (err error) {
	typeOfV := reflect.TypeOf(origin)
	valueOfV := reflect.ValueOf(origin)

	for i := 0; i < typeOfV.NumField(); i++ {
		fieldValue := valueOfV.Field(i).Interface()
		err = encodeType(buffer, fieldValue)
		if err != nil {
			return err
		}
	}
	return nil
}

func Encode(w io.Writer, origin interface{}) (err error) {
	err = encodeType(w, origin)
	return err
}
