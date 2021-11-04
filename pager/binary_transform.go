package pager

import (
	"encoding/binary"
	"fmt"
	"math"
	"minidb-go/parser/ast"
	"minidb-go/parser/token"
	"reflect"

	log "github.com/sirupsen/logrus"
)

func dumpType(buffer *[]byte, origin interface{}) (err error) {
	fieldType := reflect.TypeOf(origin).Kind()
	// reflect 性能比类型断言低
	// fieldValue := reflect.ValueOf(origin)

	// 转换 SQL 数据类型
	// 0 INT, 1 FLOAT, 2 STRING
	switch exprValue := origin.(type) {
	case ast.SQLInt:
		origin = int64(exprValue)
		dumpType(buffer, int8(0))
	case ast.SQLFloat:
		origin = float64(exprValue)
		dumpType(buffer, int8(1))
	case ast.SQLText:
		origin = string(exprValue)
		dumpType(buffer, int8(2))
	case ast.SQLColumn:
		origin = string(exprValue)
	case token.TokenType:
		origin = int(exprValue)
	}

	switch fieldType {
	case reflect.Bool:
		bitSet := origin.(bool)
		var bitSetByte byte
		if bitSet {
			bitSetByte = 1
		}
		*buffer = append(*buffer, bitSetByte)

	case reflect.Int8:
		bitSet := origin.(int8)
		var bitSetByte byte
		if bitSet == 1 {
			bitSetByte = 1
		}
		*buffer = append(*buffer, bitSetByte)

	case reflect.Int:
		buff := make([]byte, 4)
		binary.LittleEndian.PutUint32(buff, uint32(origin.(int)))
		*buffer = append(*buffer, buff...)

	case reflect.Int32:
		buff := make([]byte, 4)
		binary.LittleEndian.PutUint32(buff, uint32(origin.(int32)))
		*buffer = append(*buffer, buff...)

	case reflect.Int64:
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, uint64(origin.(int64)))
		*buffer = append(*buffer, buff...)

	case reflect.Uint64:
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, origin.(uint64))
		*buffer = append(*buffer, buff...)

	case reflect.Float64:
		bits := math.Float64bits(origin.(float64))
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, bits)
		*buffer = append(*buffer, buff...)

	case reflect.String:
		err = dumpString(buffer, origin.(string))

	case reflect.Array:
		err = dumpArray(buffer, origin)

	case reflect.Slice:
		err = dumpSlice(buffer, origin)

	case reflect.Struct:
		err = dumpStruct(buffer, origin)

	default:
		err = fmt.Errorf("dump: unsupported type: %v", fieldType)
		log.Error(err.Error())
		return
	}
	return
}

func dumpString(buffer *[]byte, str string) (err error) {
	// 先写字符串长度，再写数据
	dumpType(buffer, len(str))
	*buffer = append(*buffer, []byte(str)...)
	return nil
}

func dumpArray(buffer *[]byte, origin interface{}) (err error) {
	value := reflect.ValueOf(origin)

	for i := 0; i < value.Len(); i++ {
		err = dumpType(buffer, value.Index(i).Interface())
		if err != nil {
			return
		}
	}
	return
}

func dumpSlice(buffer *[]byte, origin interface{}) (err error) {
	value := reflect.ValueOf(origin)

	// 写入 slice 长度
	dumpType(buffer, value.Len())
	for i := 0; i < value.Len(); i++ {
		err = dumpType(buffer, value.Index(i).Interface())
		if err != nil {
			return
		}
	}
	return
}

func dumpStruct(buffer *[]byte, origin interface{}) (err error) {
	typeOfV := reflect.TypeOf(origin)
	valueOfV := reflect.ValueOf(origin)

	for i := 0; i < typeOfV.NumField(); i++ {
		fieldValue := valueOfV.Field(i).Interface()
		err = dumpType(buffer, fieldValue)
		if err != nil {
			return err
		}
	}
	return nil
}

func Dump(origin interface{}) (bin []byte, err error) {
	err = dumpType(&bin, origin)
	return bin, err
}
