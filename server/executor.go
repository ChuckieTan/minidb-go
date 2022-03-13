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
	response := &transporter.Response{
		Xid: xid,
	}
	stmt, err := parser.Parse(request.Stmt)
	if err != nil {
		response.Err = err.Error()
		return response
	}
	switch stmt := stmt.(type) {
	case ast.CreateTableStmt:
		err = tbm.CreateTable(xid, stmt)
	case ast.InsertIntoStmt:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, err = tbm.Insert(xid, stmt)
		if xid != request.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.DeleteStatement:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, err = tbm.Delete(xid, stmt)
		if xid != request.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.UpdateStmt:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, err = tbm.Update(xid, stmt)
		if xid != request.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.SelectStmt:
		if xid == 0 {
			// 开启一个临时事务
			xid = tbm.Begin()
		}
		response.ResultList, err = tbm.Select(xid, stmt)
		if xid != request.Xid {
			// 结束临时事务
			tbm.Commit(xid)
		}
	case ast.BeginStmt:
		response.Xid = tbm.Begin()
	case ast.CommitStmt:
		err = tbm.Commit(xid)
		response.Xid = 0
	case ast.RollbackStmt:
		err = tbm.Abort(xid)
		response.Xid = 0
	default:
		err = errors.New("unsupported statement")
	}
	if err != nil {
		response.Err = err.Error()
	}
	return response
}
