package ast

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"minidb-go/tm"
)

type Row struct {
	size   uint16
	offset uint64

	columns []string
	data    []SQLExprValue
}

func NewRow(data []SQLExprValue) *Row {
	row := &Row{
		data: data,
	}
	row.size = uint16(len(row.Encode()))
	return row
}

func (row *Row) SetOffset(offset uint64) {
	row.offset = offset
}

func (row *Row) Offset() uint64 {
	return row.offset
}

func (row *Row) Size() uint16 {
	return row.size
}

func (row *Row) Data() []SQLExprValue {
	return row.data
}

func (row *Row) DeepCopyData() []SQLExprValue {
	data := make([]SQLExprValue, len(row.data))
	for i, v := range row.data {
		data[i] = v.DeepCopy()
	}
	return data
}

func (row *Row) String() string {
	var buf bytes.Buffer
	// buf.WriteString("'")
	for i := 0; i < len(row.data)-3; i++ {
		buf.WriteString(fmt.Sprintf("%s ", row.data[i].String()))
	}
	buf.WriteString(row.data[len(row.data)-3].String())
	// buf.WriteString("'")
	return buf.String()
}

func (row *Row) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, row.size)
	binary.Write(buf, binary.BigEndian, row.offset)
	binary.Write(buf, binary.BigEndian, uint8(len(row.data)))
	for _, expr := range row.data {
		expr.Encode(buf)
	}
	return buf.Bytes()
}

func (row *Row) Decode(r io.Reader) error {
	binary.Read(r, binary.BigEndian, &row.size)
	binary.Read(r, binary.BigEndian, &row.offset)
	var count uint8
	binary.Read(r, binary.BigEndian, &count)
	row.data = make([]SQLExprValue, count)
	for i := uint8(0); i < count; i++ {
		val, err := decodeExprValue(r)
		if err != nil {
			return err
		}
		row.data[i] = val
	}
	return nil
}

func (row *Row) Xmin() (tm.XID, error) {
	xmin, ok := row.data[len(row.data)-2].(*SQLInt)
	if !ok {
		err := errors.New("Xmin is not int")
		return 0, err
	}
	xid := tm.XID(*xmin)
	return xid, nil
}

func (row *Row) SetXmax(xid tm.XID) {
	val := SQLInt(xid)
	row.data[len(row.data)-1] = &val
}

func (row *Row) Xmax() (tm.XID, error) {
	xmin, ok := row.data[len(row.data)-1].(*SQLInt)
	if !ok {
		err := errors.New("Xmin is not int")
		return 0, err
	}
	xid := tm.XID(*xmin)
	return xid, nil
}
