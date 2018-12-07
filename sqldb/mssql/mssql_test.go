package mssql

import (
	"fmt"
	"strings"
	"testing"
)

func TestTest(t *testing.T) {
	db := &mssql{
		connection: testConnection(),
	}
	t.Log("connection:", db.connection.SourceName())

	dbVer, err := db.Test()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("version: ", dbVer)
}

func TestMssql_Version(t *testing.T) {
	db := &mssql{
		connection: testConnection(),
	}

	t.Log("version: ", db.Version())
}

func TestMssql_SelectList(t *testing.T) {
	db := &mssql{
		connection: testConnection(),
	}
	dbEntity := &tabEntityUser{}
	err := db.SelectList(dbEntity, func() {
		t.Log("UserId:", dbEntity.UserId, "; UserName:", dbEntity.UserName)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMssql_SelectPage(t *testing.T) {
	db := &mssql{
		connection: testConnection(),
	}
	dbEntity := &tabEntityUser{}
	dbOrder := &tabEntityUserOrder{}
	dbFilter := &tabEntityUserFilter{
		Auth: 0,
	}
	sqlFilter := db.NewFilter(dbFilter, false, false)
	err := db.SelectPage(dbEntity, func(total, page, size, index uint64) {
		t.Log("total:", total, "; page:", page, "; size:", size, "; index:", index)
	}, func() {
		t.Log("UserId:", dbEntity.UserId, "; UserName:", dbEntity.UserName)
	}, 3, 2, dbOrder, sqlFilter)
	if err != nil {
		t.Fatal(err)
	}
}

func testConnection() *databaseMssql {
	return &databaseMssql{
		Server:   "172.16.99.61",
		Port:     1433,
		Schema:   "HypertensionDB",
		Instance: "MSSQLSERVER",
		User:     "sa",
		Password: "Sql2018",
		TimeOut:  10,
	}
}

type databaseMssql struct {
	Server   string `json:"server"`   // 服务器名称或IP, 默认127.0.0.1
	Port     int    `json:"port"`     // 服务器端口, 默认3306
	Instance string `json:"instance"` // 数据库实例, 默认MSSQLSERVER
	Schema   string `json:"schema"`   // 数据库名称
	User     string `json:"user"`     // 登录名
	Password string `json:"password"` // 登陆密码
	TimeOut  int    `json:"timeOut" note:"连接超时时间，单位秒，默认10"`
}

func (s *databaseMssql) DriverName() string {
	return "sqlserver"
}

func (s *databaseMssql) SourceName() string {
	// sqlserver://username:password@host/instance?param1=value&param2=value
	// sqlserver://sa@localhost/SQLExpress?database=master&connection+timeout=30 // `SQLExpress instance
	sb := strings.Builder{}
	sb.WriteString("sqlserver://")
	sb.WriteString(s.User)
	sb.WriteString(":")
	sb.WriteString(s.Password)
	sb.WriteString("@")
	sb.WriteString(s.Server)
	sb.WriteString(":")
	sb.WriteString(fmt.Sprint(s.Port))
	if len(s.Instance) > 0 {
		if strings.ToUpper(s.Instance) != "MSSQLSERVER" {
			sb.WriteString("/")
			sb.WriteString(s.Instance)
		}
	}
	sb.WriteString("?database=")
	sb.WriteString(s.Schema)
	if s.TimeOut > 0 {
		sb.WriteString("&connection+timeout=")
		sb.WriteString(fmt.Sprint(s.TimeOut))
	}

	return sb.String()
}

type TabEntityBase struct {
}

func (s TabEntityBase) TableName() string {
	return "User"
}

type tabEntityUser struct {
	TabEntityBase

	UserId   string `sql:"UserId" primary:"true"`
	UserName string `sql:"UserName"`
}

type tabEntityUserOrder struct {
	TabEntityBase

	UserName string `sql:"UserName" order:"DESC"`
}

type tabEntityUserFilter struct {
	TabEntityBase

	Auth uint64 `sql:"Auth"`
}
