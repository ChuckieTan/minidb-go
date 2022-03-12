package ast

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"minidb-go/serialization/tm"
)

type Row struct {
	Size   uint16
	Offset uint64

	Data []SQLExprValue
}

func NewRow(data []SQLExprValue) *Row {
	row := &Row{
		Data: data,
	}
	row.Size = uint16(len(row.Encode()))
	return row
}

func (row *Row) SetOffset(offset uint64) {
	row.Offset = offset
}

func (row *Row) DeepCopyData() []SQLExprValue {
	data := make([]SQLExprValue, len(row.Data))
	for i, v := range row.Data {
		data[i] = v.DeepCopy()
	}
	return data
}

func (row *Row) String() string {
	line := ""
	for i := 0; i < len(row.Data)-2; i++ {
		line += fmt.Sprintf("%s\t", row.Data[i])
	}
	return line
}

func (row *Row) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, row.Size)
	binary.Write(buf, binary.BigEndian, row.Offset)
	binary.Write(buf, binary.BigEndian, uint8(len(row.Data)))
	for _, expr := range row.Data {
		expr.Encode(buf)
	}
	return buf.Bytes()
}

func (row *Row) Decode(r io.Reader) error {
	binary.Read(r, binary.BigEndian, &row.Size)
	binary.Read(r, binary.BigEndian, &row.Offset)
	var count uint8
	binary.Read(r, binary.BigEndian, &count)
	row.Data = make([]SQLExprValue, count)
	for i := uint8(0); i < count; i++ {
		val, err := decodeExprValue(r)
		if err != nil {
			return err
		}
		row.Data[i] = val
	}
	return nil
}

func (row *Row) Xmin() (tm.XID, error) {
	xmin, ok := row.Data[len(row.Data)-2].(*SQLInt)
	if !ok {
		err := errors.New("Xmin is not int")
		return 0, err
	}
	xid := tm.XID(*xmin)
	return xid, nil
}

func (row *Row) SetXmax(xid tm.XID) {
	val := SQLInt(xid)
	row.Data[len(row.Data)-1] = &val
}

func (row *Row) Xmax() (tm.XID, error) {
	xmin, ok := row.Data[len(row.Data)-1].(*SQLInt)
	if !ok {
		err := errors.New("Xmin is not int")
		return 0, err
	}
	xid := tm.XID(*xmin)
	return xid, nil
}
