package utils

import (
	"context"
	"database/sql"
	"fmt"
)

type Table struct {
	ColumnNames   []string
	ColumnDBTypes []string
	RowValues     [][]interface{}
}

func QueryReadAll(ctx context.Context, conn *sql.DB, sqlText string, database string, schema string) (*Table, error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	if database != "" {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("use database %s", database)); err != nil {
			return nil, err
		}
	}

	if schema != "" {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("use schema %s", schema)); err != nil {
			return nil, err
		}
	}

	rows, err := tx.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, err
	}

	out := &Table{
		ColumnNames:   make([]string, 0),
		ColumnDBTypes: make([]string, 0),
		RowValues:     make([][]interface{}, 0),
	}

	colTypes, _ := rows.ColumnTypes()
	for _, t := range colTypes {
		out.ColumnNames = append(out.ColumnNames, t.Name())
		out.ColumnDBTypes = append(out.ColumnDBTypes, t.DatabaseTypeName())
	}

	for rows.Next() {
		// Result is your slice string.
		rawResult := make([][]byte, len(colTypes))
		result := make([]interface{}, len(colTypes))

		dest := make([]interface{}, len(colTypes)) // A temporary interface{} slice
		for i := range rawResult {
			dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
		}

		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}

		for i, raw := range rawResult {
			// If this was for production use, you'd cast these values as types other than string
			if raw == nil {
				result[i] = nil
			} else {
				result[i] = string(raw)
			}
		}
		out.RowValues = append(out.RowValues, result)
	}
	return out, nil
}
