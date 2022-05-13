package supervisor

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/samjbobb/mammoth/utils"
	_ "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
)

const schemaName = "mammoth_e2e"
const testConfig = `
sync:
  batchmaxitems: 100
  batchtimeout: 1s
  tables: 
    - mammoth_e2e.table1
postgres:
  slotname: mammoth_e2e
  publicationname: mammoth_e2e
snowflake:
  schema: mammoth_e2e
`

// TODO: fix goroutine leaks and re-enable this check
//func TestMain(m *testing.M) {
//	goleak.VerifyTestMain(m)
//}

func TestEnd2End(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	// Setup and start mammoth
	// Start mammoth in separate process
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		fmt.Println("cancelling")
		cancel()
	}()

	// Database connections
	pgxConn, err := pgx.Connect(ctx, os.Getenv("POSTGRES_CONNECTION"))
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer pgxConn.Close(ctx)

	sfConn, err := sql.Open("snowflake", os.Getenv("SNOWFLAKE_CONNECTION"))
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer sfConn.Close()

	pgCommands := func(cmds []string) {
		for _, cmd := range cmds {
			_, err = pgxConn.Exec(ctx, cmd)
			if !assert.NoError(t, err, cmd) {
				t.FailNow()
			}
		}
	}

	sfCommands := func(cmds []string) {
		for _, cmd := range cmds {
			_, err = sfConn.Exec(cmd)
			if !assert.NoError(t, err, cmd) {
				t.FailNow()
			}
		}
	}

	// Fresh initial state in snowflake
	sfCommands([]string{
		fmt.Sprintf(`create or replace schema %s;`, schemaName),
		fmt.Sprintf(`use schema %s;`, schemaName),
	})

	// Fresh initial state in postgres
	pgCommands([]string{
		fmt.Sprintf(`drop schema if exists %s cascade;`, schemaName),
		fmt.Sprintf(`create schema %s;`, schemaName),
		fmt.Sprintf(`set search_path to %s;`, schemaName),
		`create table table1 (id int primary key, name text);`,
		`insert into table1 (id, name) values 
		(1, 'Taylor Swift'), 
		(2, 'Van Morrison'), 
		(3, 'Woody Guthrie');`,
	})

	go func() {
		file, err := ioutil.TempFile("", "test-config.*.yml")
		panicOnErr(err)
		defer os.Remove(file.Name())

		_, err = file.WriteString(testConfig)
		panicOnErr(err)

		_, err = file.Seek(0, 0)
		panicOnErr(err)

		s := NewSupervisor()
		fmt.Println("start run")
		err = s.Run(ctx, file.Name())
		fmt.Println("end run")
		assert.ErrorIs(t, err, context.Canceled)
	}()

	// Check that snowflake reaches expected state before timeout
	{
		start := time.Now()
		var err error
		var tbl *utils.Table
		expected := &utils.Table{
			ColumnNames:   []string{"ID", "NAME"},
			ColumnDBTypes: []string{"FIXED", "TEXT"},
			RowValues: [][]interface{}{
				{"1", "Taylor Swift"},
				{"2", "Van Morrison"},
				{"3", "Woody Guthrie"},
			},
		}
		for time.Since(start) < time.Second*30 {
			fmt.Println("test polling...")
			tbl, err = utils.QueryReadAll(ctx, sfConn, `select * from table1 order by id;`, "", schemaName)
			if err == nil && reflect.DeepEqual(expected, tbl) {
				break
			}
			fmt.Println("not yet passing...")
			time.Sleep(time.Second * 3)
		}
		assert.NoError(t, err)
		assert.Equal(t, expected, tbl)
	}

	// Insert / update / delete in postgres
	pgCommands([]string{
		`insert into table1 (id, name) values
		(4, 'Saantana'),
		(5, 'Backstreet Boys');`,
		`update table1 set name = 'Santana' where id = 4;`,
		`delete from table1 where name = 'Woody Guthrie';`,
	})

	// Check that snowflake reaches expected state before timeout
	{
		start := time.Now()
		var err error
		var tbl *utils.Table
		expected := &utils.Table{
			ColumnNames:   []string{"ID", "NAME"},
			ColumnDBTypes: []string{"FIXED", "TEXT"},
			RowValues: [][]interface{}{
				{"1", "Taylor Swift"},
				{"2", "Van Morrison"},
				{"4", "Santana"},
				{"5", "Backstreet Boys"},
			},
		}
		for time.Since(start) < time.Second*30 {
			fmt.Println("test polling...")
			tbl, err = utils.QueryReadAll(ctx, sfConn, `select * from table1 order by id;`, "", schemaName)
			if err == nil && reflect.DeepEqual(expected, tbl) {
				break
			}
			fmt.Println("not yet passing...")
			time.Sleep(time.Second * 3)
		}
		assert.NoError(t, err)
		assert.Equal(t, expected, tbl)
	}
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
