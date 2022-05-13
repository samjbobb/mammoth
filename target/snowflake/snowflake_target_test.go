package snowflake

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/samjbobb/mammoth/sync/db"
	"github.com/samjbobb/mammoth/target/common/multirowaction"
	"github.com/stretchr/testify/assert"
)

// TODO: re-enable once fixed. It seems that issuing any queries against the snowflake connection results in leaked goroutines. Why?
//func TestMain(m *testing.M) {
//	goleak.VerifyTestMain(m)
//}

func TestSnowflakeTarget_createRelationStmt(t *testing.T) {
	type args struct {
		relation          *db.Relation
		tableNameOverride string
		temporary         bool
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "builds correct statement",
			args: args{
				relation: &db.Relation{
					Schema: "my_schema",
					Table:  "foo",
					Columns: []db.Column{
						{Name: "col_A", ValueType: pgtype.TextOID, IsIdentity: false},
						{Name: "col_B", ValueType: pgtype.JSONBOID, IsIdentity: false},
						{Name: "col_C", ValueType: pgtype.Int8OID, IsIdentity: false},
						{Name: "col_D", ValueType: pgtype.TextArrayOID, IsIdentity: false, IsArray: true},
					},
				},
				tableNameOverride: "",
				temporary:         false,
			},
			want: `create or replace table "X"."Y"."FOO" ("COL_A" STRING, "COL_B" VARIANT, "COL_C" INT, "COL_D" ARRAY);`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Target{database: "X", schema: "Y"}
			got := s.createTableStmt(tt.args.relation, tt.args.tableNameOverride, tt.args.temporary)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSnowflakeTarget_mergeStatement(t *testing.T) {
	relation := &db.Relation{
		Schema: schema,
		Table:  "test_table_4",
		Columns: []db.Column{
			{Name: "column_1", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_2", ValueType: pgtype.TextOID, IsIdentity: true},
			{Name: "column_3", ValueType: pgtype.TextOID, IsIdentity: false},
		},
	}

	s := Target{
		database: "DB_1",
		schema:   "SCHEMA_1",
	}

	assert.Equal(t, `merge into "DB_1"."SCHEMA_1"."TEST_TABLE_4" using "DB_1"."SCHEMA_1"."TEST_TABLE_4_SCRATCH" on
"TEST_TABLE_4"."COLUMN_1" = "TEST_TABLE_4_SCRATCH"."_IDENTITY_COLUMN_1" and "TEST_TABLE_4"."COLUMN_2" = "TEST_TABLE_4_SCRATCH"."_IDENTITY_COLUMN_2"
when matched and "TEST_TABLE_4_SCRATCH"."_DELETE_ROW" then delete
when matched then update set
	"TEST_TABLE_4"."COLUMN_1" = (case when "TEST_TABLE_4_SCRATCH"."_IS_UNCHANGED_COLUMN_1" then "TEST_TABLE_4"."COLUMN_1" when "TEST_TABLE_4_SCRATCH"."_IS_NULL_COLUMN_1" then null else "TEST_TABLE_4_SCRATCH"."COLUMN_1" end),
	"TEST_TABLE_4"."COLUMN_2" = (case when "TEST_TABLE_4_SCRATCH"."_IS_UNCHANGED_COLUMN_2" then "TEST_TABLE_4"."COLUMN_2" when "TEST_TABLE_4_SCRATCH"."_IS_NULL_COLUMN_2" then null else "TEST_TABLE_4_SCRATCH"."COLUMN_2" end),
	"TEST_TABLE_4"."COLUMN_3" = (case when "TEST_TABLE_4_SCRATCH"."_IS_UNCHANGED_COLUMN_3" then "TEST_TABLE_4"."COLUMN_3" when "TEST_TABLE_4_SCRATCH"."_IS_NULL_COLUMN_3" then null else "TEST_TABLE_4_SCRATCH"."COLUMN_3" end)
when not matched then insert (
	"COLUMN_1",
	"COLUMN_2",
	"COLUMN_3"
) values (
	case when "TEST_TABLE_4_SCRATCH"."_IS_NULL_COLUMN_1" then null else "TEST_TABLE_4_SCRATCH"."COLUMN_1" end,
	case when "TEST_TABLE_4_SCRATCH"."_IS_NULL_COLUMN_2" then null else "TEST_TABLE_4_SCRATCH"."COLUMN_2" end,
	case when "TEST_TABLE_4_SCRATCH"."_IS_NULL_COLUMN_3" then null else "TEST_TABLE_4_SCRATCH"."COLUMN_3" end);
`, s.mergeStatement(relation))
}

func Test_csvBuffer(t *testing.T) {
	type args struct {
		data *multirowaction.RelationActions
	}
	relation1 := &db.Relation{
		Schema: "foo",
		Table:  "bar",
		Columns: []db.Column{
			{Name: "col_A", ValueType: pgtype.Int8OID, IsIdentity: true},
			{Name: "col_B", ValueType: pgtype.JSONBOID, IsIdentity: false},
			{Name: "col_C", ValueType: pgtype.TextOID, IsIdentity: false},
		},
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "creates statements for put based insert",
			args: args{data: &multirowaction.RelationActions{
				Relation: relation1,
				Actions: []*db.RowAction{
					{
						Relation:           relation1,
						Kind:               "INSERT",
						ChangesRowIdentity: false,
						RowIdentityBefore: []*db.Value{
							{V: "123"}, nil, nil,
						},
						NewValues: []*db.Value{
							{V: "123"}, {V: `{"a": 1231}`}, {V: "Sam's hello A"},
						},
					},
					{
						Relation:           relation1,
						Kind:               "INSERT",
						ChangesRowIdentity: false,
						RowIdentityBefore: []*db.Value{
							{V: "456"}, nil, nil,
						},
						NewValues: []*db.Value{
							{V: "456"}, {V: "Hello, world"}, {V: `{}`},
						},
					},
				},
			}},
			want: `_IDENTITY_COL_A,_DELETE_ROW,COL_A,COL_B,COL_C,_IS_UNCHANGED_COL_A,_IS_UNCHANGED_COL_B,_IS_UNCHANGED_COL_C,_IS_NULL_COL_A,_IS_NULL_COL_B,_IS_NULL_COL_C
123,false,123,"{""a"": 1231}",Sam's hello A,false,false,false,false,false,false
456,false,456,"Hello, world",{},false,false,false,false,false,false
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columnDefinition := columnDefForLoading(tt.args.data.Relation)
			got, err := csvForStreamLoading(tt.args.data, columnDefinition)
			assert.Nil(t, err)
			assert.Equal(t, tt.want, got.String())
		})
	}
}
