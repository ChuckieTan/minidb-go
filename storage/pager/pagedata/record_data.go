package pagedata

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"minidb-go/parser/ast"
)

type RecordData struct {
	size uint16
	rows []*ast.Row
}

func NewRecordData() *RecordData {
	return &RecordData{
		size: 2 + 1,
		rows: make([]*ast.Row, 0),
	}
}

func (r *RecordData) Encode() []byte {
	buf := new(bytes.Buffer)
	buf.Grow(int(r.size))
	binary.Write(buf, binary.BigEndian, r.size)
	binary.Write(buf, binary.BigEndian, uint8(len(r.rows)))
	for _, row := range r.rows {
		buf.Write(row.Encode())
	}
	return buf.Bytes()
}

func (record *RecordData) Decode(r io.Reader) error {
	binary.Read(r, binary.BigEndian, &record.size)
	var count uint8
	err := binary.Read(r, binary.BigEndian, &count)
	if err != nil {
		log.Fatal(err)
		return err
	}
	record.rows = make([]*ast.Row, count)
	for _, row := range record.rows {
		err := row.Decode(r)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}

func (record *RecordData) Rows() []*ast.Row {
	return record.rows
}

func (record *RecordData) Size() int {
	return int(record.size)
}

func (record *RecordData) Append(row *ast.Row) {
	record.rows = append(record.rows, row)
	record.size += row.Size
}

func (record *RecordData) PageDataType() PageDataType {
	return RECORE_DATA
}
