package easysql

import (
	"reflect"
	"strings"
)

func getTypeOfArray(arr interface{}) reflect.Type {
	return reflect.TypeOf(arr).Elem().Elem()
}

type rows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...interface{}) error
}

func MapResult(rows rows, result interface{}) error {
	resultType := getTypeOfArray(result)
	writableResult := reflect.ValueOf(result).Elem()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		singleResult := reflect.New(resultType)
		reflectIndirect := reflect.Indirect(singleResult)

		var fields []interface{}
		for _, column := range columns {
			for i := 0; i < resultType.NumField(); i++ {
				if resultType.Field(i).Tag.Get(STRUCT_TAG_FIELDNAME) == column {
					fields = append(fields, reflectIndirect.Field(i).Addr().Interface())
				}
			}
		}

		err := rows.Scan(fields...)
		if err != nil {
			return err
		}
		writableResult.Set(reflect.Append(writableResult, singleResult.Elem()))
	}
	return nil
}

func ToInsertStatement(tableName string, object interface{}) (string, []interface{}) {
	reflectValue := reflect.ValueOf(object)
	reflectIndirect := reflect.Indirect(reflectValue)
	reflectType := reflectIndirect.Type()

	var fieldStringBuilder []string
	var valueStringBuilder []string
	var values []interface{}
	for i := 0; i < reflectType.NumField(); i++ {
		if reflectType.Field(i).Tag.Get(STRUCT_TAG_AUTOINCREMENT) == "true" {
			// We do not want to insert fields with auto increment
			continue
		}
		if reflectType.Field(i).Tag.Get(STRUCT_TAG_READONLY) == "true" {
			// We do not want to insert fields with read only
			continue
		}

		// Add field name
		fieldStringBuilder = append(fieldStringBuilder, reflectType.Field(i).Tag.Get(STRUCT_TAG_FIELDNAME))

		// Add parameter to the values
		valueStringBuilder = append(valueStringBuilder, "?")

		// Append ptr to the parameter list
		values = append(values, reflectIndirect.Field(i).Addr().Interface())
	}
	stmt := "INSERT INTO " + tableName + " (" + strings.Join(fieldStringBuilder, ",") + ") VALUES (" + strings.Join(valueStringBuilder, ",") + ")"

	return stmt, values
}

func ToUpdateStatement(tableName string, object interface{}, customWhere *string) (string, []interface{}) {
	reflectValue := reflect.ValueOf(object)
	reflectIndirect := reflect.Indirect(reflectValue)
	reflectType := reflectIndirect.Type()

	var partStringBuilder []string
	var primaryKeyName string
	var primaryKeyValue interface{}
	var values []interface{}
	for i := 0; i < reflectType.NumField(); i++ {
		if reflectType.Field(i).Tag.Get(STRUCT_TAG_PRIMARYKEY) == "true" {
			primaryKeyName = reflectType.Field(i).Tag.Get(STRUCT_TAG_FIELDNAME)
			primaryKeyValue = reflectIndirect.Field(i).Addr().Interface()

			// We do not want to update fields with primary key
			continue
		}

		if reflectType.Field(i).Tag.Get(STRUCT_TAG_READONLY) == "true" {
			// We do not want to update readonly fields
			continue
		}

		// Add <name> = ?
		partStringBuilder = append(partStringBuilder, reflectType.Field(i).Tag.Get(STRUCT_TAG_FIELDNAME)+" = ?")

		// Append ptr to the parameter list
		values = append(values, reflectIndirect.Field(i).Addr().Interface())
	}
	stmt := "UPDATE " + tableName + " SET " + strings.Join(partStringBuilder[:], ",")
	if customWhere != nil {
		stmt = stmt + " " + *customWhere
	} else {
		stmt = stmt + " WHERE " + primaryKeyName + " = ?"
	}
	values = append(values, primaryKeyValue)

	return stmt, values
}
