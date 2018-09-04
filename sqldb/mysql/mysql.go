package mysql

import (
	"database/sql"
	"github.com/ktpswjz/database/sqldb"

	_ "github.com/go-sql-driver/mysql"
)

type mysql struct {
	connection sqldb.SqlConnection
}

func NewDatabase(conn sqldb.SqlConnection) sqldb.SqlDatabase {
	return &mysql{connection: conn}
}

func (s *mysql) Open() (*sql.DB, error) {
	db, err := sql.Open(s.connection.DriverName(), s.connection.SourceName())
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *mysql) Test() (string, error) {
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
	db.QueryRow("SELECT VERSION()").Scan(&dbVer)

	return dbVer, nil
}

func (s *mysql) NewAccess(transactional bool) (sqldb.SqlAccess, error) {
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

func (s *mysql) NewEntity() sqldb.SqlEntity {
	return &entity{}
}

func (s *mysql) NewBuilder() sqldb.SqlBuilder {
	instance := &builder{}
	instance.Reset()

	return instance
}

func (s *mysql) NewFilter(entity interface{}, fieldOr, groupOr bool) sqldb.SqlFilter {
	return newFilter(entity, fieldOr, groupOr)
}

func (s *mysql) IsNoRows(err error) bool {
	if err == nil {
		return false
	}

	if err == sql.ErrNoRows {
		return true
	}

	return false
}

func (s *mysql) Insert(entity interface{}) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.Insert(entity)
}

func (s *mysql) InsertSelective(entity interface{}) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.InsertSelective(entity)
}

func (s *mysql) Delete(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.Delete(entity, filters...)
}

func (s *mysql) Update(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.Update(entity, filters...)
}

func (s *mysql) UpdateSelective(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.UpdateSelective(entity, filters...)
}

func (s *mysql) SelectOne(entity interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectOne(entity, filters...)
}

func (s *mysql) SelectDistinct(entity interface{}, row func(), order interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectDistinct(entity, row, order, filters...)
}

func (s *mysql) SelectList(entity interface{}, row func(), order interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectList(entity, row, order, filters...)
}

func (s *mysql) SelectPage(entity interface{}, page func(total, page, size, index uint64), row func(), size, index uint64, order interface{}, filters ...sqldb.SqlFilter) error {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectPage(entity, page, row, size, index, order, filters...)
}

func (s *mysql) SelectCount(entity interface{}, filters ...sqldb.SqlFilter) (uint64, error) {
	sqlAccess, err := s.NewAccess(false)
	if err != nil {
		return 0, err
	}
	defer sqlAccess.Close()

	return sqlAccess.SelectCount(entity, filters...)
}
