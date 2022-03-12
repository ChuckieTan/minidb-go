package tbm

import (
	"fmt"
	"minidb-go/parser/ast"
)

type ResultList struct {
	Columns []string
	Rows    []*ast.Row
}

func (result *ResultList) String() string {
	if len(result.Columns) == 0 {
		return "\n"
	}
	str := ""
	for _, column := range result.Columns {
		str += column + "\t"
	}
	str += "\n"
	for _, row := range result.Rows {
		str += fmt.Sprintf("%s\n", row)
	}
	str += "\n"
	return str
}

func (tbm *TableManager) NewResultList(tableName string, rows []*ast.Row) (*ResultList, error) {
	tableInfo := tbm.metaData.GetTableInfo(tableName)
	if tableInfo == nil {
		return nil, ErrTableNotExists
	}
	columns := tableInfo.ColumnNames()
	return &ResultList{
		Columns: columns,
		Rows:    rows,
	}, nil
}
