package database

import (
	"database/sql"
	"errors"
	"strconv"
)

type SqlDatabase interface {
	Execute(query string) error
	Select(result interface{}, query string, args ...interface{}) error
	Count(query string, args ...interface{}) (uint64, error)
	Insert(tableName string, object interface{}) (int64, error)
	Update(tableName string, object interface{}, customWhere *string) error
	Close()
}

type sqlDatabaseImpl struct {
	dbHandle *sql.DB
}

func CreateAndConnect(host string, port uint16, dbname string, user string, password string) (*sqlDatabaseImpl, error) {
	db, err := sql.Open("mysql", user+":"+password+"@tcp("+host+":"+strconv.Itoa(int(port))+")/"+dbname+"?parseTime=true")
	if err != nil {
		return nil, err
	}

	return &sqlDatabaseImpl{dbHandle: db}, nil
}

func (db *sqlDatabaseImpl) Close() {
	db.dbHandle.Close()
}

func (db *sqlDatabaseImpl) Select(result interface{}, query string, args ...interface{}) error {
	rows, err := db.dbHandle.Query(query, args...)
	if err != nil {
		return err
	}

	return MapResult(rows, result)
}

type CountResult struct {
	Value uint64 `db_fieldname:"result"`
}

func (db *sqlDatabaseImpl) Count(query string, args ...interface{}) (uint64, error) {
	rows, err := db.dbHandle.Query(query, args...)
	if err != nil {
		return 0, err
	}

	var result []CountResult
	err = MapResult(rows, &result)
	if err != nil {
		return 0, err
	}
	if len(result) == 0 {
		return 0, errors.New("SqlDatabase: Count() result has no field with name 'result'")
	}
	return result[0].Value, nil
}

func (db *sqlDatabaseImpl) Insert(tableName string, object interface{}) (int64, error) {
	statement, values := ToInsertStatement(tableName, object)
	result, err := db.dbHandle.Exec(statement, values...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (db *sqlDatabaseImpl) Update(tableName string, object interface{}, customWhere *string) error {
	statement, values := ToUpdateStatement(tableName, object, customWhere)
	_, err := db.dbHandle.Exec(statement, values...)
	return err
}

func (db *sqlDatabaseImpl) Execute(query string) error {
	_, err := db.dbHandle.Exec(query)
	return err
}
