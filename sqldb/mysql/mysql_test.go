package mysql

import (
	"fmt"
	"testing"
)

func TestTest(t *testing.T) {
	db := NewDatabase(testConnection())

	dbVer, err := db.Test()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("version: ", dbVer)
}

func testConnection() *databaseMysql {
	return &databaseMysql{
		Server:   "172.16.99.231",
		Port:     3306,
		Schema:   "test",
		Charset:  "utf8",
		User:     "dev",
		Password: "pwd",
	}
}

type databaseMysql struct {
	Server   string `json:"server"`   // 服务器名称或IP, 默认127.0.0.1
	Port     int    `json:"port"`     // 服务器端口, 默认3306
	Schema   string `json:"schema"`   // 数据库名称, 默认mtps
	Charset  string `json:"charset"`  // 字符集, 默认utf8
	User     string `json:"user"`     // 登录名
	Password string `json:"password"` // 登陆密码
}

func (s *databaseMysql) DriverName() string {
	return "mysql"
}

func (s *databaseMysql) SourceName() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		s.User,
		s.Password,
		s.Server,
		s.Port,
		s.Schema,
		s.Charset)
}
