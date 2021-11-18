package database

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type DummyObject struct {
	Id        uint64    `db_fieldname:"id" db_primarykey:"true" db_autoincrement:"true"`
	Name      string    `db_fieldname:"name"`
	CreatedAt time.Time `db_fieldname:"created_at"`
}

// ------------------- getTypeOfArray() -----------------------------------

func TestGetTypeOfArray(t *testing.T) {
	var someString = "someValue"
	assert.Panics(t, func() { getTypeOfArray(someString) }, "A string value is not an array")
	var someInt = 123
	assert.Panics(t, func() { getTypeOfArray(someInt) }, "An int value is not an array")
	var someStringArray []string
	assert.Panics(t, func() { getTypeOfArray(someStringArray) }, "An array must be a pointer")
	getTypeOfArray(&someStringArray)
}

// ------------------- MapResult() -----------------------------------

type MockRows struct {
	SqlOutput []DummyObject
	index     int
}

func (mr *MockRows) Columns() ([]string, error) {
	return []string{"name", "created_at", "id"}, nil
}
func (mr *MockRows) Next() bool {
	return mr.index < len(mr.SqlOutput)
}
func (mr *MockRows) Scan(dest ...interface{}) error {
	so := mr.SqlOutput[mr.index]
	reflect.ValueOf(dest[0]).Elem().Set(reflect.ValueOf(so.Name))
	reflect.ValueOf(dest[1]).Elem().Set(reflect.ValueOf(so.CreatedAt))
	reflect.ValueOf(dest[2]).Elem().Set(reflect.ValueOf(so.Id))
	mr.index++
	return nil
}

func TestMapResultEmpty(t *testing.T) {
	// GIVEN
	rows := &MockRows{SqlOutput: []DummyObject{}}
	var result []DummyObject

	// WHEN
	err := MapResult(rows, &result)

	// THEN
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result))
}

func TestMapResult(t *testing.T) {
	// GIVEN
	sqlOutput := []DummyObject{
		{Id: 1, Name: "John", CreatedAt: time.Now()},
		{Id: 2, Name: "Jane", CreatedAt: time.Now()},
		{Id: 3, Name: "Mary", CreatedAt: time.Now()},
	}
	rows := &MockRows{SqlOutput: sqlOutput}
	var result []DummyObject

	// WHEN
	err := MapResult(rows, &result)

	// THEN
	assert.Nil(t, err)
	assert.Equal(t, 3, len(result))
	for i, so := range sqlOutput {
		assert.Equal(t, so.Id, result[i].Id)
		assert.Equal(t, so.Name, result[i].Name)
		assert.Equal(t, so.CreatedAt, result[i].CreatedAt)
	}
}

// ------------------- ToInsertStatement() -----------------------------------

func TestToInsertStatement(t *testing.T) {
	// GIVEN
	tableName := "tableName"
	currentTime := time.Now()
	object := DummyObject{Name: "John", CreatedAt: currentTime}

	// WHEN
	stmt, values := ToInsertStatement(tableName, &object)

	// THEN
	assert.Equal(t, "INSERT INTO "+tableName+" (name,created_at) VALUES (?,?)", stmt)
	assert.Equal(t, 2, len(values))
	assert.Equal(t, "John", reflect.ValueOf(values[0]).Elem().Interface())
	assert.Equal(t, currentTime, reflect.ValueOf(values[1]).Elem().Interface())
}

// ------------------- ToUpdateStatement() -----------------------------------

func TestToUpdateStatement(t *testing.T) {
	// GIVEN
	tableName := "tableName"
	currentTime := time.Now()
	object := DummyObject{Id: 1, Name: "John", CreatedAt: currentTime}

	// WHEN
	stmt, values := ToUpdateStatement(tableName, &object, nil)

	// THEN
	assert.Equal(t, "UPDATE "+tableName+" SET name = ?,created_at = ? WHERE id = ?", stmt)
	assert.Equal(t, 3, len(values))
	assert.Equal(t, "John", reflect.ValueOf(values[0]).Elem().Interface())
	assert.Equal(t, currentTime, reflect.ValueOf(values[1]).Elem().Interface())
	assert.Equal(t, uint64(1), reflect.ValueOf(values[2]).Elem().Interface())
}
