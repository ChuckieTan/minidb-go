package server

import (
	"errors"
	"minidb-go/parser"
	"minidb-go/parser/ast"
	"minidb-go/tbm"
	"minidb-go/transporter"
)

func ExecuteStmt(tbm *tbm.TableManager, request *transporter.Request) *transporter.Response {
	xid := request.Xid
	response := &transporter.Response{}
	stmt, err := parser.Parse(request.Stmt)
	if err != nil {
		response.Err = err
		return response
	}
	switch stmt := stmt.(type) {
	case ast.CreateTableStmt:
		response.Err = tbm.CreateTable(xid, stmt)
	case ast.InsertIntoStmt:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, response.Err = tbm.Insert(xid, stmt)
		if xid != response.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.DeleteStatement:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, response.Err = tbm.Delete(xid, stmt)
		if xid != response.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.UpdateStmt:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, response.Err = tbm.Update(xid, stmt)
		if xid != response.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.SelectStmt:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, response.Err = tbm.Select(xid, stmt)
		if xid != response.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.BeginStmt:
		response.Xid = tbm.Begin()
	case ast.CommitStmt:
		response.Err = tbm.Commit(xid)
	case ast.RollbackStmt:
		response.Err = tbm.Abort(xid)
	default:
		response.Err = errors.New("unsupported statement")
	}
	return response
}
