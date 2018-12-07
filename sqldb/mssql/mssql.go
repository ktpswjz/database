package mssql

import (
	"database/sql"
	"github.com/ktpswjz/database/sqldb"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

type mssql struct {
	connection sqldb.SqlConnection
}

func NewDatabase(conn sqldb.SqlConnection) sqldb.SqlDatabase {
	return &mssql{connection: conn}
}

func (s *mssql) Open() (*sql.DB, error) {
	db, err := sql.Open(s.connection.DriverName(), s.connection.SourceName())
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *mssql) Test() (string, error) {
	db, err := sql.Open(s.connection.DriverName(), s.connection.SourceName())
	if err != nil {
		return "", err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		return "", err
	}

	dbVer := ""
	db.QueryRow("SELECT @@VERSION").Scan(&dbVer)
	index := strings.Index(dbVer, "\n")
	if index > 0 {
		dbVer = dbVer[0:index]
	}

	return dbVer, nil
}

func (s *mssql) Version() int {
	version, err := s.Test()
	if err != nil {
		return 0
	}

	vs := strings.Split(version, " ")
	if len(vs) > 3 {
		v, err := strconv.Atoi(vs[3])
		if err == nil {
			return v
		}
	}

	return 0
}

func (s *mssql) NewAccess(transactional bool) (sqldb.SqlAccess, error) {
	db, err := sql.Open(s.connection.DriverName(), s.connection.SourceName())
	if err != nil {
		return nil, err
	}

	if transactional {
		tx, err := db.Begin()
		if err != nil {
			db.Close()
			return nil, err
		}

		return &transaction{db: db, tx: tx}, nil
	}

	return &normal{db: db}, nil
}

func (s *mssql) NewEntity() sqldb.SqlEntity {
	return &entity{}
}

func (s *mssql) NewBuilder() sqldb.SqlBuilder {
	instance := &builder{}
	instance.Reset()

	return instance
}

func (s *mssql) NewFilter(entity interface{}, fieldOr, groupOr bool) sqldb.SqlFilter {
	return newFilter(entity, fieldOr, groupOr)
}

func (s *mssql) IsNoRows(err error) bool {
	if err == nil {
		return false
	}

	if err == sql.ErrNoRows {
		return true
	}

	return false
}

func (s *mssql) Insert(entity interface{}) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.Insert(entity)
}

func (s *mssql) InsertSelective(entity interface{}) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.InsertSelective(entity)
}

func (s *mssql) Delete(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.Delete(entity, filters...)
}

func (s *mssql) Update(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.Update(entity, filters...)
}

func (s *mssql) UpdateSelective(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.UpdateSelective(entity, filters...)
}

func (s *mssql) UpdateByPrimaryKey(entity interface{}) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.UpdateByPrimaryKey(entity)
}

func (s *mssql) UpdateSelectiveByPrimaryKey(entity interface{}) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.UpdateSelectiveByPrimaryKey(entity)
}

func (s *mssql) SelectOne(entity interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectOne(entity, filters...)
}

func (s *mssql) SelectDistinct(entity interface{}, row func(), order interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectDistinct(entity, row, order, filters...)
}

func (s *mssql) SelectList(entity interface{}, row func(), order interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectList(entity, row, order, filters...)
}

func (s *mssql) SelectPage(entity interface{}, page func(total, page, size, index uint64), row func(), size, index uint64, order interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectPage(entity, page, row, size, index, order, filters...)
}

func (s *mssql) SelectCount(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectCount(entity, filters...)
}
