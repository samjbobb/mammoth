package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/samjbobb/mammoth/sync/db"
	"github.com/samjbobb/mammoth/utils"
	"github.com/stretchr/testify/assert"
)

var schema = ""

func init() {
	schema = fmt.Sprintf("snowflake_target_integration_test_%s", strings.Replace(uuid.New().String(), "-", "_", -1))
}

func TestSnowflakeTargetIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode.")
	}

	type initialize struct {
		relation       *db.Relation
		source         io.Reader
		sqlTextForWant string
		wantAfter      *utils.Table
	}

	type writeAndWant struct {
		transactionBatch func() []*db.WalTransaction
		sqlTextForWant   string
		wantAfter        func() *utils.Table
	}

	t1Relation := db.Relation{
		Schema: schema,
		Table:  "test_table_1",
		Columns: []db.Column{
			{Name: "column_1", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_2", ValueType: pgtype.TextOID, IsIdentity: false},
		},
	}

	t2Relation := db.Relation{
		Schema: schema,
		Table:  "test_table_2",
		Columns: []db.Column{
			{Name: "column_1", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_2", ValueType: pgtype.TextOID, IsIdentity: false},
		},
	}

	t3Relation := db.Relation{
		Schema: schema,
		Table:  "test_table_3",
		Columns: []db.Column{
			{Name: "column_1", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_2", ValueType: pgtype.JSONBOID, IsIdentity: false},
			{Name: "column_3", ValueType: pgtype.Int8OID, IsIdentity: false},
			{Name: "column_4", ValueType: pgtype.Float4OID, IsIdentity: false},
			{Name: "column_5", ValueType: pgtype.BoolOID, IsIdentity: false},
			{Name: "column_6", ValueType: pgtype.TextArrayOID, IsIdentity: false, IsArray: true},
		},
	}

	t4Relation := db.Relation{
		Schema: schema,
		Table:  "test_table_4",
		Columns: []db.Column{
			{Name: "column_1", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_2", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_3", ValueType: pgtype.TextOID, IsIdentity: false},
		},
	}

	t5Relation := db.Relation{
		Schema: schema,
		Table:  "test_table_5",
		Columns: []db.Column{
			{Name: "column_1", ValueType: pgtype.Int4OID, IsIdentity: true},
			{Name: "column_2", ValueType: pgtype.TextOID, IsIdentity: false},
			{Name: "column_3", ValueType: pgtype.TextArrayOID, IsIdentity: false, IsArray: true},
		},
	}

	t6Relation := db.Relation{
		Schema: schema,
		Table:  "test_table_6",
		Columns: []db.Column{
			{Name: "column_1", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_2", ValueType: pgtype.TextOID, IsIdentity: false},
			{Name: "column_3", ValueType: pgtype.TextOID, IsIdentity: false},
		},
	}

	tests := []struct {
		name          string
		initialize    initialize
		writeAndWants []writeAndWant
	}{
		{
			name: "update same row multiple times in one batch",
			initialize: initialize{
				relation: &t1Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2"},
					{"1", "apple"},
				}),
				sqlTextForWant: "select * from test_table_1 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
					ColumnDBTypes: []string{"TEXT", "TEXT"},
					RowValues: [][]interface{}{
						{"1", "apple"},
					},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						return []*db.WalTransaction{
							{Actions: []*db.RowAction{
								{
									Relation:           &t1Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil},
									NewValues:          []*db.Value{{V: "1"}, {V: "pear"}},
								},
								{
									Relation:           &t1Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil},
									NewValues:          []*db.Value{{V: "1"}, {V: "pear"}},
								},
								{
									Relation:           &t1Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil},
									NewValues:          []*db.Value{{V: "1"}, {V: "pear"}},
								},
								{
									Relation:           &t1Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil},
									NewValues:          []*db.Value{{V: "1"}, {V: "pickle"}},
								},
							}}}
					},
					sqlTextForWant: "select * from test_table_1 order by column_1;",
					wantAfter: func() *utils.Table {
						return &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
							ColumnDBTypes: []string{"TEXT", "TEXT"},
							RowValues: [][]interface{}{
								{"1", "pickle"},
							},
						}
					},
				},
			},
		},
		{
			name: "insert nulls",
			initialize: initialize{
				relation: &t2Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2"},
					{"1", "apple"},
					// Empty field is null in CSV
					{"2", ""},
				}),
				sqlTextForWant: "select * from test_table_2 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
					ColumnDBTypes: []string{"TEXT", "TEXT"},
					RowValues: [][]interface{}{
						{"1", "apple"},
						{"2", nil},
					},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						return []*db.WalTransaction{
							{Actions: []*db.RowAction{
								{
									Relation:           &t2Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil},
									NewValues:          []*db.Value{{V: "1"}, {V: "", Null: true}},
								},
							}}}
					},
					sqlTextForWant: "select * from test_table_2 order by column_1;",
					wantAfter: func() *utils.Table {
						return &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
							ColumnDBTypes: []string{"TEXT", "TEXT"},
							RowValues: [][]interface{}{
								{"1", nil},
								{"2", nil},
							},
						}
					},
				},
			},
		},
		{
			name: "types: json, int, float, bool",
			initialize: initialize{
				relation: &t3Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2", "column_3", "column_4", "column_5", "column_6"},
					{"1", `{"a": 123, "b": "double quote: \" backslash: \\"}`, "8", "5.5", "true", "{foo,bar,bim}"},
					// Empty field is null in CSV
					{"2", "", "", "", "", ""},
				}),
				sqlTextForWant: "select * from test_table_3 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3", "COLUMN_4", "COLUMN_5", "COLUMN_6"},
					ColumnDBTypes: []string{"TEXT", "VARIANT", "FIXED", "REAL", "BOOLEAN", "ARRAY"},
					RowValues: [][]interface{}{
						{"1", `{
  "a": 123,
  "b": "double quote: \" backslash: \\"
}`, "8", "5.500000", "true", `[
  "foo",
  "bar",
  "bim"
]`},
						{"2", nil, nil, nil, nil, nil},
					},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						return []*db.WalTransaction{
							{Actions: []*db.RowAction{
								{
									Relation:           &t3Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil, nil, nil, nil, nil},
									NewValues:          []*db.Value{{V: "1"}, {Null: true}, {Null: true}, {Null: true}, {Null: true}, {Null: true}},
								},
								{
									Relation:           &t3Relation,
									Kind:               db.Insert,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "3"}, nil, nil, nil, nil, nil},
									NewValues:          []*db.Value{{V: "3"}, {V: `{ "a": 123, "b": "double quote: \" backslash: \\"}`}, {V: "10"}, {V: "1.2"}, {V: "false"}, {V: "{{one,two},{three,four}}"}},
								},
							}}}
					},
					sqlTextForWant: "select * from test_table_3 order by column_1;",
					wantAfter: func() *utils.Table {
						return &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3", "COLUMN_4", "COLUMN_5", "COLUMN_6"},
							ColumnDBTypes: []string{"TEXT", "VARIANT", "FIXED", "REAL", "BOOLEAN", "ARRAY"},
							RowValues: [][]interface{}{
								{"1", nil, nil, nil, nil, nil},
								{"2", nil, nil, nil, nil, nil},
								{"3", `{
  "a": 123,
  "b": "double quote: \" backslash: \\"
}`, "10", "1.200000", "false", `[
  [
    "one",
    "two"
  ],
  [
    "three",
    "four"
  ]
]`},
							},
						}
					},
				},
			},
		},
		{
			name: "update many rows with merge",
			initialize: initialize{
				relation: &t2Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2"},
					{"1", "apple"},
					{"2", "pear"},
					{"3", "peach"},
					{"4", "carrot"},
				}),
				sqlTextForWant: "select * from test_table_2 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
					ColumnDBTypes: []string{"TEXT", "TEXT"},
					RowValues: [][]interface{}{
						{"1", "apple"},
						{"2", "pear"},
						{"3", "peach"},
						{"4", "carrot"},
					},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						return []*db.WalTransaction{
							{Actions: []*db.RowAction{
								{
									Relation:           &t2Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil},
									NewValues:          []*db.Value{{V: "1"}, {V: "Apple"}},
								},
								{
									Relation:           &t2Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "2"}, nil},
									NewValues:          []*db.Value{{V: "2"}, {V: "Pear"}},
								},
								{
									Relation:           &t2Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "3"}, nil},
									NewValues:          []*db.Value{{V: "3"}, {V: "Peach"}},
								},
								{
									Relation:           &t2Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "4"}, nil},
									NewValues:          []*db.Value{{V: "4"}, {V: "Carrot"}},
								},
							}}}
					},
					sqlTextForWant: "select * from test_table_2 order by column_1;",
					wantAfter: func() *utils.Table {
						return &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
							ColumnDBTypes: []string{"TEXT", "TEXT"},
							RowValues: [][]interface{}{
								{"1", "Apple"},
								{"2", "Pear"},
								{"3", "Peach"},
								{"4", "Carrot"},
							},
						}
					},
				},
			},
		},
		{
			name: "delete rows with one key column",
			initialize: initialize{
				relation: &t2Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2"},
					{"1", "apple"},
					{"2", "pear"},
					{"3", "peach"},
					{"4", "carrot"},
				}),
				sqlTextForWant: "select * from test_table_2 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
					ColumnDBTypes: []string{"TEXT", "TEXT"},
					RowValues: [][]interface{}{
						{"1", "apple"},
						{"2", "pear"},
						{"3", "peach"},
						{"4", "carrot"},
					},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						return []*db.WalTransaction{
							{Actions: []*db.RowAction{
								{
									Relation:           &t2Relation,
									Kind:               db.Delete,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, {V: "", Null: true}},
									NewValues:          nil,
								},
								{
									Relation:           &t2Relation,
									Kind:               db.Delete,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "2"}, {V: "", Null: true}},
									NewValues:          nil,
								},
							}}}
					},
					sqlTextForWant: "select * from test_table_2 order by column_1;",
					wantAfter: func() *utils.Table {
						return &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2"},
							ColumnDBTypes: []string{"TEXT", "TEXT"},
							RowValues: [][]interface{}{
								{"3", "peach"},
								{"4", "carrot"},
							},
						}
					},
				},
			},
		},
		{
			name: "delete rows with multiple key columns",
			initialize: initialize{
				relation: &t4Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2", "column_3"},
					{"1", "a", "apple"},
					{"2", "b", "pear"},
					{"3", "c", "peach"},
					{"4", "d", "carrot"},
				}),
				sqlTextForWant: "select * from test_table_4 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
					ColumnDBTypes: []string{"TEXT", "TEXT", "TEXT"},
					RowValues: [][]interface{}{
						{"1", "a", "apple"},
						{"2", "b", "pear"},
						{"3", "c", "peach"},
						{"4", "d", "carrot"},
					},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						return []*db.WalTransaction{
							{Actions: []*db.RowAction{
								{
									Relation:           &t4Relation,
									Kind:               db.Delete,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, {V: "a"}, {V: "", Null: true}},
									NewValues:          nil,
								},
								{
									Relation:           &t4Relation,
									Kind:               db.Delete,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "2"}, {V: "b"}, {V: "", Null: true}},
									NewValues:          nil,
								},
							}}}
					},
					sqlTextForWant: "select * from test_table_4 order by column_1;",
					wantAfter: func() *utils.Table {
						return &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
							ColumnDBTypes: []string{"TEXT", "TEXT", "TEXT"},
							RowValues: [][]interface{}{
								{"3", "c", "peach"},
								{"4", "d", "carrot"},
							},
						}
					},
				},
			},
		},
		{
			name: "insert, update, delete many rows with put",
			initialize: initialize{
				relation: &t5Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2", "column_3"},
				}),
				sqlTextForWant: "select * from test_table_5 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
					ColumnDBTypes: []string{"FIXED", "TEXT", "ARRAY"},
					RowValues:     [][]interface{}{},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						rows := []*db.WalTransaction{
							{Actions: []*db.RowAction{}}}
						for i := 0; i < 100; i++ {
							rows[0].Actions = append(rows[0].Actions, &db.RowAction{
								Relation:           &t5Relation,
								Kind:               db.Insert,
								ChangesRowIdentity: false,
								RowIdentityBefore:  []*db.Value{{V: fmt.Sprintf("%d", i)}, nil, nil},
								NewValues:          []*db.Value{{V: fmt.Sprintf("%d", i)}, {V: fmt.Sprintf("apple-%d", i)}, {V: fmt.Sprintf("{apple%d}", i)}},
							})
						}
						return rows
					},
					sqlTextForWant: "select * from test_table_5 order by column_1;",
					wantAfter: func() *utils.Table {
						t := &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
							ColumnDBTypes: []string{"FIXED", "TEXT", "ARRAY"},
							RowValues:     [][]interface{}{},
						}
						for i := 0; i < 100; i++ {
							t.RowValues = append(t.RowValues,
								[]interface{}{fmt.Sprintf("%d", i), fmt.Sprintf("apple-%d", i), fmt.Sprintf("[\n  \"apple%d\"\n]", i)})
						}
						return t
					},
				},
				{
					transactionBatch: func() []*db.WalTransaction {
						rows := []*db.WalTransaction{
							{Actions: []*db.RowAction{}}}
						for i := 0; i < 100; i++ {
							rows[0].Actions = append(rows[0].Actions, &db.RowAction{
								Relation:           &t5Relation,
								Kind:               db.Update,
								ChangesRowIdentity: false,
								RowIdentityBefore:  []*db.Value{{V: fmt.Sprintf("%d", i)}, nil, nil},
								NewValues:          []*db.Value{{V: fmt.Sprintf("%d", i)}, {V: fmt.Sprintf("pear-%d", i)}, {V: fmt.Sprintf("{pear%d}", i)}},
							})
						}
						return rows
					},
					sqlTextForWant: "select * from test_table_5 order by column_1;",
					wantAfter: func() *utils.Table {
						t := &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
							ColumnDBTypes: []string{"FIXED", "TEXT", "ARRAY"},
							RowValues:     [][]interface{}{},
						}
						for i := 0; i < 100; i++ {
							t.RowValues = append(t.RowValues,
								[]interface{}{fmt.Sprintf("%d", i), fmt.Sprintf("pear-%d", i), fmt.Sprintf("[\n  \"pear%d\"\n]", i)})
						}
						return t
					},
				},
				{
					transactionBatch: func() []*db.WalTransaction {
						rows := []*db.WalTransaction{
							{Actions: []*db.RowAction{}}}
						for i := 20; i < 100; i++ {
							rows[0].Actions = append(rows[0].Actions, &db.RowAction{
								Relation:           &t5Relation,
								Kind:               db.Delete,
								ChangesRowIdentity: false,
								RowIdentityBefore:  []*db.Value{{V: fmt.Sprintf("%d", i)}, {Null: true}, {Null: true}},
								NewValues:          nil,
							})
						}
						return rows
					},
					sqlTextForWant: "select * from test_table_5 order by column_1;",
					wantAfter: func() *utils.Table {
						t := &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
							ColumnDBTypes: []string{"FIXED", "TEXT", "ARRAY"},
							RowValues:     [][]interface{}{},
						}
						for i := 0; i < 20; i++ {
							t.RowValues = append(t.RowValues,
								[]interface{}{fmt.Sprintf("%d", i), fmt.Sprintf("pear-%d", i), fmt.Sprintf("[\n  \"pear%d\"\n]", i)})
						}
						return t
					},
				},
			},
		},
		{
			name: "update with values marked unchanged",
			initialize: initialize{
				relation: &t6Relation,
				source: csvForInit([][]string{
					{"column_1", "column_2", "column_3"},
					{"1", "apple", "farm"},
				}),
				sqlTextForWant: "select * from test_table_6 order by column_1;",
				wantAfter: &utils.Table{
					ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
					ColumnDBTypes: []string{"TEXT", "TEXT", "TEXT"},
					RowValues: [][]interface{}{
						{"1", "apple", "farm"},
					},
				},
			},
			writeAndWants: []writeAndWant{
				{
					transactionBatch: func() []*db.WalTransaction {
						return []*db.WalTransaction{
							{Actions: []*db.RowAction{
								{
									Relation:           &t6Relation,
									Kind:               db.Update,
									ChangesRowIdentity: false,
									RowIdentityBefore:  []*db.Value{{V: "1"}, nil, nil},
									NewValues:          []*db.Value{{V: "1"}, {V: "pear"}, {Unchanged: true}},
								},
							}}}
					},
					sqlTextForWant: "select * from test_table_6 order by column_1;",
					wantAfter: func() *utils.Table {
						return &utils.Table{
							ColumnNames:   []string{"COLUMN_1", "COLUMN_2", "COLUMN_3"},
							ColumnDBTypes: []string{"TEXT", "TEXT", "TEXT"},
							RowValues: [][]interface{}{
								{"1", "pear", "farm"},
							},
						}
					},
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sfConn, err := sql.Open("snowflake", os.Getenv("SNOWFLAKE_CONNECTION"))
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer func() {
		utils.PanicIfErr(sfConn.Close())
	}()

	snowflakePublisher, err := NewTarget(ctx, sfConn, "", schema)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	defer func() {
		err := snowflakePublisher.Close(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = snowflakePublisher.InitializeRelation(ctx, tt.initialize.relation, tt.initialize.source)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			if tt.initialize.sqlTextForWant != "" {
				got, err := utils.QueryReadAll(ctx, sfConn, tt.initialize.sqlTextForWant, "", schema)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !assert.Equal(t, tt.initialize.wantAfter, got) {
					t.FailNow()
				}
			}

			for idx, step := range tt.writeAndWants {
				err = snowflakePublisher.Write(ctx, step.transactionBatch())
				if !assert.NoError(t, err, "writeAndWants step", idx) {
					t.FailNow()
				}
				if step.sqlTextForWant != "" {
					got, err := utils.QueryReadAll(ctx, sfConn, step.sqlTextForWant, "", schema)
					if !assert.NoError(t, err) {
						t.FailNow()
					}
					if !assert.Equal(t, step.wantAfter(), got) {
						t.FailNow()
					}
				}
			}
		})
	}
}

func csvForInit(rows [][]string) *bytes.Buffer {
	buf := &bytes.Buffer{}
	w := csv.NewWriter(buf)
	for _, row := range rows {
		if err := w.Write(row); err != nil {
			panic(err)
		}
	}
	w.Flush()
	return buf
}
