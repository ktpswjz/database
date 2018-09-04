package sqldb

import (
	"database/sql"
)

type SqlConnection interface {
	DriverName() string
	SourceName() string
}

type SqlDatabase interface {
	Test() (string, error)

	NewAccess(transactional bool) (SqlAccess, error)
	NewEntity() SqlEntity
	NewBuilder() SqlBuilder
	NewFilter(entity interface{}, fieldOr, groupOr bool) SqlFilter

	IsNoRows(err error) bool
	Insert(entity interface{}) (uint64, error)
	InsertSelective(entity interface{}) (uint64, error)
	Delete(entity interface{}, filters ...SqlFilter) (uint64, error)
	Update(entity interface{}, filters ...SqlFilter) (uint64, error)
	UpdateSelective(entity interface{}, filters ...SqlFilter) (uint64, error)
	SelectCount(entity interface{}, filters ...SqlFilter) (uint64, error)
	SelectOne(entity interface{}, filters ...SqlFilter) error
	SelectDistinct(entity interface{}, row func(), order interface{}, filters ...SqlFilter) error
	SelectList(entity interface{}, row func(), order interface{}, filters ...SqlFilter) error
	SelectPage(entity interface{}, page func(total, page, size, index uint64), row func(), size, index uint64, order interface{}, filters ...SqlFilter) error
}

type SqlAccess interface {
	Close() error
	Commit() error

	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row

	IsNoRows(err error) bool
	Insert(entity interface{}) (uint64, error)
	InsertSelective(entity interface{}) (uint64, error)
	Delete(entity interface{}, filters ...SqlFilter) (uint64, error)
	Update(entity interface{}, filters ...SqlFilter) (uint64, error)
	UpdateSelective(entity interface{}, filters ...SqlFilter) (uint64, error)
	SelectCount(entity interface{}, filters ...SqlFilter) (uint64, error)
	SelectOne(entity interface{}, filters ...SqlFilter) error
	SelectDistinct(entity interface{}, row func(), order interface{}, filters ...SqlFilter) error
	SelectList(entity interface{}, row func(), order interface{}, filters ...SqlFilter) error
	SelectPage(entity interface{}, page func(total, page, size, index uint64), row func(), size, index uint64, order interface{}, filters ...SqlFilter) error
}

type SqlField interface {
	Name() string
	Value() interface{}
	Address() interface{}
	AutoIncrement() bool
	Filter() string
	Order() string
	ValueEmpty() bool
}

type SqlEntity interface {
	Parse(entity interface{}) error
	ParseFilter(entity interface{}) error
	Name() string
	FieldCount() int
	Field(i int) SqlField
	ScanFields() string
	ScanArgs() []interface{}
	Values() []interface{}
}

type SqlBuilder interface {
	Query() string
	Args() []interface{}

	Reset() SqlBuilder
	Select(query string, distinct bool) SqlBuilder
	Insert(query string) SqlBuilder
	Delete(query string) SqlBuilder
	Update(query string) SqlBuilder
	From(query string) SqlBuilder
	Value(filed string, value interface{}) SqlBuilder
	Set(filed string, value interface{}) SqlBuilder
	WhereFormatAnd(format string, a ...interface{}) SqlBuilder
	WhereFormatOr(format string, a ...interface{}) SqlBuilder
	WhereFormat(format string, a ...interface{}) SqlBuilder
	WhereAnd(query string, args ...interface{}) SqlBuilder
	WhereOr(query string, args ...interface{}) SqlBuilder
	Where(query string, args ...interface{}) SqlBuilder
	Order(query string) SqlBuilder
	Append(query string, args ...interface{}) SqlBuilder
	AppendFormat(format string, a ...interface{}) SqlBuilder
}

type SqlFilter interface {
	FieldOr() bool
	GroupOr() bool
	Fields() interface{}
}