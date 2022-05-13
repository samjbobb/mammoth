package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/samjbobb/mammoth/sync/db"
	"github.com/samjbobb/mammoth/target/common/arraytojson"
	"github.com/samjbobb/mammoth/target/common/csvsplit"
	"github.com/samjbobb/mammoth/target/common/multirowaction"
	"github.com/samjbobb/mammoth/utils"
	"github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"
)

const (
	rowsPerChunk   = 20000
	copyFileFormat = `file_format = (type = 'CSV' field_delimiter = ',' field_optionally_enclosed_by='"' skip_header = 1)`
)

type Target struct {
	conn      *sql.DB
	warehouse string
	database  string
	schema    string
}

var mergeStatementTemplate *template.Template

func init() {
	mergeStatementTemplate = compileStatementTemplate()
}

func NewTarget(ctx context.Context, conn *sql.DB, database string, schema string) (*Target, error) {
	t := &Target{conn: conn}

	if strings.HasPrefix(database, `"`) {
		database = strings.Trim(database, `"`)
	} else {
		database = strings.ToUpper(database)
	}
	if strings.HasPrefix(schema, `"`) {
		schema = strings.Trim(schema, `"`)
	} else {
		schema = strings.ToUpper(schema)
	}

	var curWh string
	err := conn.QueryRowContext(ctx,
		"select ifnull(current_warehouse(), '');").
		Scan(&curWh)
	if err != nil {
		return nil, err
	}
	t.warehouse = curWh

	if err := t.setupDatabase(ctx, database); err != nil {
		return nil, err
	}

	if err := t.setupSchema(ctx, schema); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Target) setupDatabase(ctx context.Context, database string) error {
	if database == "" {
		var curDb string
		err := t.conn.QueryRowContext(ctx,
			"select ifnull(current_database(), '');").
			Scan(&curDb)
		if err != nil {
			return err
		}
		logrus.Infoln("using current database from session:", curDb)
		t.database = curDb
		return nil
	}

	exists, err := t.anyRows(ctx, fmt.Sprintf(`show databases like '%s';`, database))
	if err != nil {
		return err
	}
	if !exists {
		logrus.Infoln("database does not exist, creating...")
		if _, err := t.conn.ExecContext(ctx, fmt.Sprintf("create database if not exists %s;", database)); err != nil {
			return err
		}
	}
	t.database = database
	return nil
}

func (t *Target) setupSchema(ctx context.Context, schema string) error {
	if schema == "" {
		var curSch string
		err := t.conn.QueryRowContext(ctx,
			"select ifnull(current_schema(), '');").
			Scan(&curSch)
		if err != nil {
			return err
		}
		logrus.Infoln("using current schema from session:", curSch)
		t.schema = curSch
		return nil
	}

	exists, err := t.anyRows(ctx, fmt.Sprintf(`show schemas like '%s' in database "%s";`, schema, t.database))
	if err != nil {
		return err
	}
	if !exists {
		logrus.Infoln("schema does not exist, creating...")
		if _, err := t.conn.ExecContext(ctx, fmt.Sprintf(`create schema if not exists "%s"."%s";`, t.database, schema)); err != nil {
			return err
		}
	}
	t.schema = schema
	return nil
}

func (t *Target) String() string {
	return fmt.Sprintf("snowflake target using warehouse: %s, database: %s, schema: %s", t.warehouse, t.database, t.schema)
}

// InitializeRelation creates a relation and populates it with initial data from source
// In order to avoid leaving any relation in an intermediate state if interrupted by errors, the strategy is to
// create a table with `_SCRATCH` appended to the name and load data to the scratch table. When loading is complete,
// atomically make the table live (by removing the _SCRATCH) suffix.
// This would be simpler if it could be done in a transaction, but Snowflake ends any current
// transaction as soon as a DDL statement is executed (like CREATE TABLE or ALTER TABLE).
// https://docs.snowflake.com/en/sql-reference/transactions.html#ddl
func (t *Target) InitializeRelation(ctx context.Context, relation *db.Relation, source io.Reader) error {
	if _, err := t.conn.Exec(t.createTableStmt(relation, t.tableNameFq(relation, true), false)); err != nil {
		return fmt.Errorf("could not create table: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chunks, splitErrs, err := csvsplit.Split(ctx, rowsPerChunk, true, source, &arrayToJsonXformer{relation: relation})
	if err != nil {
		return err
	}

	opId := uuid.New().String()

	sinkErrs := make(chan error, 1)
	go func() {
		defer close(sinkErrs)
		fileNum := 0
		for chunk := range chunks {
			sqlText := fmt.Sprintf("put 'file://%s-%s-%d.csv' %s auto_compress=true parallel=30 overwrite=true;",
				t.safeTableIdentifier(relation), opId, fileNum, t.tableStage(relation, true))
			logrus.Infoln(sqlText)
			if _, err := t.conn.ExecContext(gosnowflake.WithFileStream(ctx, chunk),
				sqlText); err != nil {
				sinkErrs <- fmt.Errorf("could not put: %w", err)
				return
			}
			fileNum++
		}
	}()

	if err := utils.WaitForPipeline(ctx, splitErrs, sinkErrs); err != nil {
		return fmt.Errorf("error while uploading csv: %w", err)
	}

	sqlText := fmt.Sprintf(`copy into %s from %s pattern='.*/%s-%s-.*[.]csv.*' %s;`,
		t.tableNameFq(relation, true), t.tableStage(relation, true), t.safeTableIdentifier(relation), opId, copyFileFormat)
	logrus.Infoln(sqlText)
	if _, err := t.conn.Exec(sqlText); err != nil {
		return fmt.Errorf("could not copy: %w", err)
	} else {
		logrus.Infof("finished copy into %s\n", t.tableNameFq(relation, true))
	}

	// if table exists: swap. if not: rename
	targetTableParts := t.tableNameParts(relation, false)
	exists, err := t.anyRows(ctx, fmt.Sprintf(`show tables like '%s' in schema "%s"."%s";`, targetTableParts[2], t.database, t.schema))
	if err != nil {
		return err
	}
	if exists {
		sqlText = fmt.Sprintf(`alter table %s swap with %s;`, t.tableNameFq(relation, true), t.tableNameFq(relation, false))
	} else {
		sqlText = fmt.Sprintf(`alter table %s rename to %s;`, t.tableNameFq(relation, true), t.tableNameFq(relation, false))
	}
	logrus.Debugln(sqlText)
	if _, err := t.conn.Exec(sqlText); err != nil {
		return fmt.Errorf("could not rename/swap table: %w", err)
	} else {
		logrus.Infof("new table live %s\n", t.tableNameFq(relation, false))
	}

	// drop the scratch table
	if exists {
		if _, err := t.conn.Exec(fmt.Sprintf(`drop table %s;`, t.tableNameFq(relation, true))); err != nil {
			return fmt.Errorf("could not drop scratch table: %w", err)
		}
	}

	return nil
}

func (t *Target) VerifyRelation(ctx context.Context, relation *db.Relation) (bool, error) {
	// TODO: centralize functionality to translate from PG schema + table name to Snowflake schema + table name
	tx, err := t.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("could not start transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			logrus.WithError(err).Warnln("rollback")
		}
	}()

	if _, err := tx.Exec(t.createTableStmt(relation, t.tableNameFq(relation, true), true)); err != nil {
		return false, fmt.Errorf("could not create temporary table: %w", err)
	}

	if _, err := tx.Exec(fmt.Sprintf(`show columns in schema %s.%s;`, t.database, t.schema)); err != nil {
		return false, fmt.Errorf("could not show columns: %w", err)
	}

	table := t.tableNameParts(relation, false)[2]
	tmpTable := t.tableNameParts(relation, true)[2]
	rows, err := tx.Query(`
	with cols as (
		select * from table (result_scan(last_query_id()))
	)
	select "column_name", "data_type", case when count(*) = 1 then 'only in: ' || any_value(location) else 'both' end as status
	from (
        select "column_name", "data_type", 'snowflake' as location from cols where "table_name" = ?
        union all
        select "column_name", "data_type", 'postgres' as location from cols where "table_name" = ?
    ) group by 1, 2 order by 1;`, table, tmpTable)

	if err != nil {
		return false, err
	}
	defer func() {
		utils.PanicIfErr(rows.Close())
	}()

	ok := true
	for rows.Next() {
		var column string
		var dataType string
		var status string
		if err := rows.Scan(&column, &dataType, &status); err != nil {
			return false, err
		}
		if status != "both" {
			logrus.WithFields(logrus.Fields{"column": column, "dataType": dataType, "status": status}).Warnln("postgres and snowflake schema mismatch")
			ok = false
		}
	}
	return ok, nil
}

// Write pushes events from postgres into snowflake.
// Each batch is a list of complete postgres transactions (so referential integrity will be maintained
// after a complete batch).
// Each action affects one row, even if a single statement was issued in postgres that affected more than one row.
// For example, `INSERT INTO items(id) SELECT i FROM generate_series(0, 100) as t(i);` is one postgres statement
// that inserts 100 rows. This would generate an Actions object with 100 individual Insert actions.
// In order to execute this efficiently in Snowflake, we must group actions to execute multiple actions as a single
// snowflake statement.
func (t *Target) Write(ctx context.Context, batch []*db.WalTransaction) error {
	start := time.Now()
	if len(batch) == 0 {
		logrus.Debugln("no actions to publish")
		return nil
	}
	// Each group stores changes for one table
	changeGroups, err := multirowaction.Grouper(batch)
	if err != nil {
		return err
	}
	// Events are grouped by table, and it's possible for one PG transaction to
	// affect multiple tables, so wrap these operations in a Snowflake transaction
	// to avoid creating inconsistency while tables are updated sequentially
	tx, err := t.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not start transaction: %w", err)
	}
	numStatements := 0
	for _, changeGroup := range changeGroups {
		logrus.WithFields(logrus.Fields{"numChangeGroups": len(changeGroup.Actions), "tableName": changeGroup.Table}).Infoln("begin write changes for table")
		if len(changeGroup.Actions) == 0 {
			continue
		}
		statementGroup, err := t.statements(changeGroup)
		if err != nil {
			return err
		}
		logrus.WithFields(logrus.Fields{"statements": len(statementGroup)}).Debugln("run statementGroup")
		// The Snowflake library doesn't work when combining WithFileStream and WithMultiStatement, so execute each
		// statement individually because the file stream is needed
		for _, statement := range statementGroup {
			if statement.stream != nil {
				ctx = gosnowflake.WithFileStream(ctx, statement.stream)
			}
			if _, err := tx.ExecContext(ctx, statement.sqlText); err != nil {
				return fmt.Errorf("could not execute statement: %s: %w", statement.sqlText, err)
			}
		}
		numStatements += len(statementGroup)
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	numRowChanges := 0
	for _, tx := range batch {
		numRowChanges += len(tx.Actions)
	}
	logrus.
		WithFields(logrus.Fields{
			"numRowChanges":   numRowChanges,
			"numStatements":   numStatements,
			"numTransactions": len(batch),
			"took":            time.Since(start).String()}).
		Infof("batch published to Snowflake")
	return nil
}

func (t *Target) Close(context.Context) error {
	return t.conn.Close()
}

func (t *Target) statements(actions *multirowaction.RelationActions) ([]*statementWithStream, error) {
	tmpTableName := t.tableNameFq(actions.Relation, true)
	columnDefinition := columnDefForLoading(actions.Relation)
	createTableStmt, err := t.statementToCreateTableForStreamLoading(tmpTableName, columnDefinition)
	if err != nil {
		return nil, err
	}
	csvBuf, err := csvForStreamLoading(actions, columnDefinition)
	if err != nil {
		return nil, err
	}
	fileName := fmt.Sprintf("%s-%s.csv", t.safeTableIdentifier(actions.Relation), uuid.New().String())

	return []*statementWithStream{
		{sqlText: createTableStmt},
		{sqlText: fmt.Sprintf("put 'file://%s' %s auto_compress=true parallel=30 overwrite=true;",
			strings.ReplaceAll(fileName, "\\", "\\\\"), t.tableStage(actions.Relation, true)), stream: csvBuf},
		{sqlText: fmt.Sprintf(`copy into %s from %s/%s %s;`, tmpTableName, t.tableStage(actions.Relation, true), fileName, copyFileFormat)},
		{sqlText: t.mergeStatement(actions.Relation)},
	}, nil
}

func (t *Target) createTableStmt(relation *db.Relation, tableNameOverride string, temporary bool) string {
	table := t.tableNameFq(relation, false)
	if tableNameOverride != "" {
		table = tableNameOverride
	}
	var sfColDef []string

	for _, col := range relation.Columns {
		sfColDef = append(sfColDef, fmt.Sprintf(`"%s" %s`, t.columnName(col), getSfType(col)))
	}
	tableType := "table"
	if temporary {
		tableType = "temporary table"
	}
	return fmt.Sprintf(`create or replace %s %s (%s);`, tableType, table, strings.Join(sfColDef, ", "))
}

func (t *Target) statementToCreateTableForStreamLoading(tableName string, columnDefinition []*streamColumnDef) (string, error) {
	columnNames := make([]string, len(columnDefinition))
	for idx, colDef := range columnDefinition {
		// TODO: unify with s.columnName instead of ToUpper
		columnNames[idx] = strings.ToUpper(colDef.columnName)
	}
	columnTypes := make([]string, len(columnDefinition))
	for idx, colDef := range columnDefinition {
		columnTypes[idx] = colDef.dbType
	}

	if len(columnNames) != len(columnTypes) {
		return "", errors.New("columnNames and columnTypes must have same length")
	}
	sfColDef := make([]string, len(columnNames))
	for idx, columnName := range columnNames {
		sfColDef[idx] = fmt.Sprintf(`"%s" %s`, columnName, columnTypes[idx])
	}
	return fmt.Sprintf(`create or replace temporary table %s (%s);`, tableName, strings.Join(sfColDef, ", ")), nil
}

func csvForStreamLoading(actions *multirowaction.RelationActions, columnDefinition []*streamColumnDef) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	var csvHeaders []string
	for _, col := range columnDefinition {
		csvHeaders = append(csvHeaders, col.columnName)
	}
	if err := writer.Write(csvHeaders); err != nil {
		return nil, fmt.Errorf("could not write to csv buffer: %w", err)
	}
	for _, action := range actions.Actions {
		rowValues := make([]string, len(columnDefinition))
		for colIdx, col := range columnDefinition {
			var err error
			rowValues[colIdx], err = col.valueGetter(action)
			if err != nil {
				return nil, err
			}
		}
		if err := writer.Write(rowValues); err != nil {
			return nil, fmt.Errorf("could not write to csv buffer: %w", err)
		}
	}
	writer.Flush()
	return buf, nil
}

type streamColumnDef struct {
	columnName  string
	valueGetter func(action *db.RowAction) (string, error)
	dbType      string
}

func columnDefForLoading(relation *db.Relation) []*streamColumnDef {
	var csvColumns []*streamColumnDef
	// Columns:
	// * _identity_{columnName} -> value of identity columns before action
	//   - ... additional identity columns ...
	// * _delete_row -> boolean flag indicating that row should be deleted
	// * {columnName} -> new values for column
	//   - ... additional value column for each column in table ...
	// _is_unchanged_{columnName) -> boolean flag indicating that the existing value for this column should be maintained, new value is empty
	//   - ... additional _is_unchanged column for each column in table ...
	// _is_null_{columnName} -> boolean flag indicating that the new value is null
	//   - ... additional _is_null column for each column in the table
	for _, colIdx := range relation.IdentityColumnIndices() {
		csvColumns = append(csvColumns, &streamColumnDef{
			columnName: fmt.Sprintf(`_IDENTITY_%s`, strings.ToUpper(relation.Columns[colIdx].Name)),
			// The extra closure is required here to capture the value of colIdx
			// https://github.com/golang/go/wiki/CommonMistakes
			valueGetter: func(i int) func(action *db.RowAction) (string, error) {
				return func(action *db.RowAction) (string, error) {
					return action.RowIdentityBefore[i].V, nil
				}
			}(colIdx),
			dbType: getSfType(relation.Columns[colIdx]),
		})
	}
	csvColumns = append(csvColumns, &streamColumnDef{
		columnName: "_DELETE_ROW",
		valueGetter: func(action *db.RowAction) (string, error) {
			return strconv.FormatBool(action.Kind == db.Delete), nil
		},
		dbType: "BOOLEAN",
	})
	for colIdx, col := range relation.Columns {
		csvColumns = append(csvColumns, &streamColumnDef{
			columnName: strings.ToUpper(col.Name),
			valueGetter: func(i int) func(action *db.RowAction) (string, error) {
				return func(action *db.RowAction) (string, error) {
					if action.Kind == db.Delete {
						return "", nil
					} else if !relation.Columns[i].IsArray {
						return action.NewValues[i].V, nil
					} else {
						arrVal, err := arraytojson.PGArrayToJSON(action.NewValues[i].V)
						if err != nil {
							return "", err
						}
						return arrVal, nil
					}
				}
			}(colIdx),
			dbType: getSfType(relation.Columns[colIdx]),
		})
	}
	for colIdx, col := range relation.Columns {
		csvColumns = append(csvColumns, &streamColumnDef{
			// TODO: use s.ColumnName here instead of strings.ToUpper
			// TODO: Unify the creation of these column names with the creation in mergeStatement
			columnName: fmt.Sprintf(`_IS_UNCHANGED_%s`, strings.ToUpper(col.Name)),
			valueGetter: func(i int) func(action *db.RowAction) (string, error) {
				return func(action *db.RowAction) (string, error) {
					if action.Kind == db.Delete {
						return "", nil
					}
					return strconv.FormatBool(action.NewValues[i].Unchanged), nil
				}
			}(colIdx),
			dbType: "BOOLEAN",
		})
	}
	for colIdx, col := range relation.Columns {
		csvColumns = append(csvColumns, &streamColumnDef{
			columnName: fmt.Sprintf(`_IS_NULL_%s`, strings.ToUpper(col.Name)),
			valueGetter: func(i int) func(action *db.RowAction) (string, error) {
				return func(action *db.RowAction) (string, error) {
					if action.Kind == db.Delete {
						return "", nil
					}
					return strconv.FormatBool(action.NewValues[i].Null), nil
				}
			}(colIdx),
			dbType: "BOOLEAN",
		})
	}
	return csvColumns
}

func separator(s string) func() string {
	i := -1
	return func() string {
		i++
		if i == 0 {
			return ""
		}
		return s
	}
}

func compileStatementTemplate() *template.Template {
	tmpl := `
{{- $target := .TargetTable -}}
{{- $source := .SourceTable -}}
merge into {{.TargetTableFq}} using {{.SourceTableFq}} on
{{ $s := separator " and " -}}
{{- $first := true -}}
{{- range .IdentityColumns -}}
	{{ call $s }}
	{{- $target}}."{{ columnName .}}" = {{$source}}."_IDENTITY_{{ columnName .}}"
{{- end}}
when matched and {{$source}}."_DELETE_ROW" then delete
when matched then update set
{{- $s = separator "," -}}
{{- range .Columns -}}
	{{call $s}}
	{{$target}}."{{ columnName .}}" = (case when {{$source}}."_IS_UNCHANGED_{{ columnName .}}" then {{$target}}."{{ columnName .}}" when {{$source}}."_IS_NULL_{{ columnName .}}" then null else {{$source}}."{{ columnName .}}" end)
{{- end}}
when not matched then insert (
{{- $s = separator "," -}}
{{- range .Columns -}}
	{{call $s}}
	"{{ columnName .}}"
{{- end}}
) values (
{{- $s = separator "," -}}
{{- range .Columns -}}
	{{call $s}}
	case when {{$source}}."_IS_NULL_{{ columnName .}}" then null else {{$source}}."{{ columnName .}}" end
{{- end -}}
);
`
	t, err := template.New("merge").Funcs(template.FuncMap{
		"separator": separator,
		// TODO: unify this with s.ColumnName
		"columnName": strings.ToUpper,
	}).Parse(tmpl)
	if err != nil {
		panic(err)
	}
	return t
}

func (t *Target) mergeStatement(relation *db.Relation) string {
	templateContext := struct {
		TargetTableFq   string
		TargetTable     string
		SourceTableFq   string
		SourceTable     string
		IdentityColumns []string
		Columns         []string
	}{
		TargetTableFq:   t.tableNameFq(relation, false),
		TargetTable:     t.tableNameOnly(relation, false),
		SourceTableFq:   t.tableNameFq(relation, true),
		SourceTable:     t.tableNameOnly(relation, true),
		IdentityColumns: relation.IdentityColumnNames(),
		Columns:         relation.ColumnNames(),
	}

	var buf bytes.Buffer
	err := mergeStatementTemplate.Execute(&buf, templateContext)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func (t *Target) anyRows(ctx context.Context, sqlText string) (bool, error) {
	rows, err := t.conn.QueryContext(ctx, sqlText)
	if err != nil {
		return false, err
	}
	defer func() {
		utils.PanicIfErr(rows.Close())
	}()
	return rows.Next(), rows.Err()
}

func (t *Target) tableNameParts(relation *db.Relation, scratch bool) [3]string {
	// TODO: more advanced mapping of postgres table to snowflake table here
	// In this function we have the snowflake database, snowflake schema, postgres schema, postgres table
	tbl := strings.ToUpper(relation.Table)
	if scratch {
		tbl = fmt.Sprintf("%s_SCRATCH", tbl)
	}
	return [3]string{t.database, t.schema, tbl}
}

// tableNameFq returns the fully qualified and quoted Snowflake table name, given a Postgres relation
func (t *Target) tableNameFq(relation *db.Relation, scratch bool) string {
	parts := t.tableNameParts(relation, scratch)
	return fmt.Sprintf(`"%s"."%s"."%s"`, parts[0], parts[1], parts[2])
}

// tableNameOnly returns the quoted Snowflake table name only, given a Postgres relation
func (t *Target) tableNameOnly(relation *db.Relation, scratch bool) string {
	parts := t.tableNameParts(relation, scratch)
	return fmt.Sprintf(`"%s"`, parts[2])
}

func (t *Target) tableStage(relation *db.Relation, scratch bool) string {
	parts := t.tableNameParts(relation, scratch)
	return fmt.Sprintf(`@"%s"."%s"."%%%s"`, parts[0], parts[1], parts[2])
}

// safeTableIdentifier returns a string that uniquely identifies a postgres table and is safe for use as a SQL
// identifier or file name without quotes
func (t *Target) safeTableIdentifier(relation *db.Relation) string {
	return fmt.Sprintf(`%s_%s`, strings.ToUpper(relation.Schema), strings.ToUpper(relation.Table))
}

func (t *Target) columnName(column db.Column) string {
	return strings.ToUpper(column.Name)
}

type arrayToJsonXformer struct {
	relation *db.Relation
}

func (a *arrayToJsonXformer) Transform(in []string) ([]string, error) {
	if len(a.relation.Columns) != len(in) {
		return nil, fmt.Errorf("column number mismatch between csv and relation: %v %v", a.relation, in)
	}
	out := make([]string, len(in))
	for idx, col := range a.relation.Columns {
		if !col.IsArray {
			out[idx] = in[idx]
			continue
		}
		elOut, err := arraytojson.PGArrayToJSON(in[idx])
		if err != nil {
			return nil, err
		}
		out[idx] = elOut
	}
	return out, nil
}

func getSfType(col db.Column) string {
	// https://github.com/jackc/pgtype is a good source of info about these types
	var typeMapping = map[uint32]string{
		pgtype.BoolOID:             "BOOLEAN",
		pgtype.Int8OID:             "INT",
		pgtype.Int2OID:             "INT",
		pgtype.Int4OID:             "INT",
		pgtype.JSONOID:             "VARIANT",
		pgtype.CIDRArrayOID:        "ARRAY",
		pgtype.Float4OID:           "FLOAT4",
		pgtype.Float8OID:           "FLOAT8",
		pgtype.BoolArrayOID:        "ARRAY",
		pgtype.Int2ArrayOID:        "ARRAY",
		pgtype.Int4ArrayOID:        "ARRAY",
		pgtype.TextArrayOID:        "ARRAY",
		pgtype.ByteaArrayOID:       "ARRAY",
		pgtype.BPCharArrayOID:      "ARRAY",
		pgtype.VarcharArrayOID:     "ARRAY",
		pgtype.Int8ArrayOID:        "ARRAY",
		pgtype.Float4ArrayOID:      "ARRAY",
		pgtype.Float8ArrayOID:      "ARRAY",
		pgtype.ACLItemArrayOID:     "ARRAY",
		pgtype.InetArrayOID:        "ARRAY",
		pgtype.DateOID:             "DATE",
		pgtype.TimestampOID:        "TIMESTAMP_NTZ", // TODO: are we handling timezones correctly
		pgtype.TimestampArrayOID:   "ARRAY",
		pgtype.DateArrayOID:        "ARRAY",
		pgtype.TimestamptzOID:      "TIMESTAMP_TZ",
		pgtype.TimestamptzArrayOID: "ARRAY",
		pgtype.JSONBOID:            "VARIANT",
	}
	if col.IsArray {
		return "ARRAY"
	}
	sfType, exists := typeMapping[col.ValueType]
	if exists {
		return sfType
	}
	return "STRING"
}

type statementWithStream struct {
	sqlText string
	stream  io.Reader
}
