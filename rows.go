// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/xiaofengshuyu/odbc/api"
)

type Rows struct {
	os *ODBCStmt
}

func (r *Rows) Columns() []string {
	names := make([]string, len(r.os.Cols))
	for i := 0; i < len(names); i++ {
		names[i] = r.os.Cols[i].Name()
	}
	return names
}

func (r *Rows) Next(dest []driver.Value) error {
	ret := api.SQLFetch(r.os.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLFetch", r.os.h)
	}
	for i := range dest {
		v, err := r.os.Cols[i].Value(r.os.h, i)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

func (r *Rows) Close() error {
	return r.os.closeByRows()
}

func (r *Rows) HasNextResultSet() bool {
	return true
}

func (r *Rows) NextResultSet() error {
	ret := api.SQLMoreResults(r.os.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLMoreResults", r.os.h)
	}

	err := r.os.BindColumns()
	if err != nil {
		return err
	}
	return nil
}

var (
	scanTypeNullFloat  = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt    = reflect.TypeOf(sql.NullInt64{})
	scanTypeNullString = reflect.TypeOf(sql.NullString{})
	scanTypeNullBool   = reflect.TypeOf(sql.NullBool{})
	scanTypeNullTime   = reflect.TypeOf(NullTime{})
	scanTypeRawBytes   = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown    = reflect.TypeOf(new(interface{}))
)

func (r *Rows) ColumnTypeScanType(i int) reflect.Type {
	switch x := r.os.Cols[i].(type) {
	case *BindableColumn:
		return cTypeScanType(x.CType)
	case *NonBindableColumn:
		return cTypeScanType(x.CType)
	}
	return scanTypeUnknown
}

func cTypeScanType(ctype api.SQLSMALLINT) reflect.Type {
	switch ctype {
	case api.SQL_C_BIT:
		return scanTypeNullBool
	case api.SQL_C_LONG:
		return scanTypeNullInt
	case api.SQL_C_SBIGINT:
		return scanTypeNullInt
	case api.SQL_C_DOUBLE:
		return scanTypeNullFloat
	case api.SQL_C_CHAR:
		return scanTypeNullString
	case api.SQL_C_WCHAR:
		return scanTypeNullString
	case api.SQL_C_TYPE_TIMESTAMP:
		return scanTypeNullTime
	case api.SQL_C_GUID:
		return scanTypeNullTime
	case api.SQL_C_DATE:
		return scanTypeNullTime
	case api.SQL_C_TIME:
		return scanTypeNullTime
	case api.SQL_C_BINARY:
		if ctype == api.SQL_SS_TIME2 {
			return scanTypeNullTime
		}
		return scanTypeRawBytes
	default:
		return scanTypeUnknown
	}
}
