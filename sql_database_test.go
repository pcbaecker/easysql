package database

import (
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

type User struct {
	Id        uint64     `json:"id" db_fieldname:"id" db_autoincrement:"true" db_primarykey:"true"`
	Email     *string    `json:"email" db_fieldname:"email"`
	Name      *string    `json:"name" db_fieldname:"name"`
	CreatedAt *time.Time `json:"createdAt" db_fieldname:"created_at" db_readonly:"true"`
	UpdatedAt *time.Time `json:"updatedAt" db_fieldname:"updated_at" db_readonly:"true"`
}

func TestAll(t *testing.T) {
	if os.Getenv("DB_HOST") == "" {
		// We cant execute this test without db
		assert.Fail(t, "A mysql db is required to execute this test!")
		return
	}

	db, err := CreateAndConnect(
		os.Getenv("DB_HOST"),
		3306,
		os.Getenv("DB_NAME"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"))
	assert.NoError(t, err)
	// Create
	tableName := "dummy_table"
	err = db.Execute("CREATE TABLE " + tableName + ` (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		email varchar(64) DEFAULT NULL,
		name varchar(64) DEFAULT NULL,
		created_at datetime DEFAULT CURRENT_TIMESTAMP,
		updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id)
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)
	assert.NoError(t, err)
	// Select empty
	var users []User
	err = db.Select(&users, "SELECT * FROM "+tableName)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(users))
	// Insert
	for i := 0; i < 10; i++ {
		name := "John" + strconv.Itoa(i)
		email := strconv.Itoa(i) + "@me.com"
		_, err := db.Insert(tableName, &User{Name: &name, Email: &email})
		assert.NoError(t, err)
	}
	time.Sleep(3 * time.Second) // We want to wait so the update timestamp can differ from the created one
	// Count
	numberOfRows, err := db.Count("SELECT COUNT(*) as result FROM " + tableName)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), numberOfRows)
	// Select all
	err = db.Select(&users, "SELECT * FROM "+tableName)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(users))
	for i, user := range users {
		assert.Equal(t, uint64(i+1), user.Id)
		assert.Equal(t, "John"+strconv.Itoa(i), *user.Name)
		assert.Equal(t, strconv.Itoa(i)+"@me.com", *user.Email)
		assert.Greater(t, user.CreatedAt.Unix(), int64(0))
		assert.Greater(t, user.UpdatedAt.Unix(), int64(0))
	}
	// Select first
	var singleUser []User
	err = db.Select(&singleUser, "SELECT * FROM "+tableName+" WHERE id = ?", 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(singleUser))
	// Update first
	*singleUser[0].Name = "Jane"
	err = db.Update(tableName, &singleUser[0], nil)
	assert.NoError(t, err)
	// Select first user again
	var singleUserAgain []User
	err = db.Select(&singleUserAgain, "SELECT * FROM "+tableName+" WHERE id = ?", 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(singleUserAgain))
	assert.Greater(t, singleUserAgain[0].UpdatedAt.Unix(), singleUser[0].UpdatedAt.Unix())
	assert.Greater(t, singleUserAgain[0].UpdatedAt.Unix(), singleUserAgain[0].CreatedAt.Unix())
	// Drop
	err = db.Execute("DROP TABLE " + tableName)
	assert.NoError(t, err)
}
