package db

import (
	"bytes"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

type Relation struct {
	Schema  string
	Table   string
	Columns []Column
}

type Column struct {
	Name       string
	ValueType  uint32
	IsIdentity bool
	IsArray    bool
}

type Value struct {
	V    string
	Null bool
	// Values that are TOASTed are not sent when they are unchanged, instead this flag is set.
	// The other columns of the row should be updated but this value should remain unchanged.
	Unchanged bool
}

// ActionKind is the kind of RowAction
type ActionKind string

const (
	Insert ActionKind = "INSERT"
	Update ActionKind = "UPDATE"
	Delete ActionKind = "DELETE"
)

func (k ActionKind) string() string {
	return string(k)
}

func Map(vs []*Value, f func(int, *Value) *Value) []*Value {
	vsm := make([]*Value, len(vs))
	for i, v := range vs {
		vsm[i] = f(i, v)
	}
	return vsm
}

func (r *Relation) Equal(r2 *Relation) bool {
	if r.Schema != r2.Schema || r.Table != r2.Table {
		return false
	}
	if len(r.Columns) != len(r2.Columns) {
		return false
	}
	for idx, col := range r.Columns {
		col2 := r2.Columns[idx]
		if col.Name != col2.Name || col.IsIdentity != col2.IsIdentity || col.ValueType != col2.ValueType {
			return false
		}
	}
	return true
}

func (r *Relation) IdentityColumns() []Column {
	out := make([]Column, 0)
	for _, col := range r.Columns {
		if col.IsIdentity {
			out = append(out, col)
		}
	}
	return out
}

func (r *Relation) IdentityColumnIndices() []int {
	out := make([]int, 0)
	for idx, col := range r.Columns {
		if col.IsIdentity {
			out = append(out, idx)
		}
	}
	return out
}

func (r *Relation) NotIdentityColumns() []Column {
	out := make([]Column, 0)
	for _, col := range r.Columns {
		if !col.IsIdentity {
			out = append(out, col)
		}
	}
	return out
}

func (r *Relation) ColumnNames() []string {
	out := make([]string, len(r.Columns))
	for idx, col := range r.Columns {
		out[idx] = col.Name
	}
	return out
}

func (r *Relation) IdentityColumnNames() []string {
	identityColumns := r.IdentityColumns()
	out := make([]string, len(identityColumns))
	for idx, col := range identityColumns {
		out[idx] = col.Name
	}
	return out
}

// WalTransaction transaction specified WAL message.
type WalTransaction struct {
	LSN        pglogrepl.LSN
	BeginTime  *time.Time
	CommitTime *time.Time
	Actions    []*RowAction
}

// RowAction records a change to a row
// Kind identifies the type of change (INSERT, UPDATE, DELETE)
// ChangesRowIdentity is set true when postgres reports that an UPDATE has changed the row identity (key)
// RowIdentityBefore and NewValues are both arrays of values, the length of these arrays must be equal to
// the number of columns in the Relation.
// RowIdentityBefore holds the row identity values for the row before this action.
//  Non-identity columns are set to nil.
// NewValues holds the values for the row after this action. It is nil for DELETE actions.
type RowAction struct {
	*Relation
	Kind               ActionKind
	ChangesRowIdentity bool
	RowIdentityBefore  []*Value
	NewValues          []*Value
}

func (a *RowAction) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s %s %s\n", a.Schema, a.Table, a.Kind))
	for colIdx, col := range a.Columns {
		buf.WriteString(fmt.Sprintf("    name: %v, isIdentity: %v, ValueType: %v, oldValue: %v, newValue: %v\n",
			col.Name, col.IsIdentity, col.ValueType, a.RowIdentityBefore[colIdx], a.NewValues[colIdx]))
	}
	return buf.String()
}

func (a *RowAction) BeforeActionRowIdentity() string {
	buf := bytes.Buffer{}
	for colIdx := range a.IdentityColumnIndices() {
		// TODO: possible bug if RowIdentityBefore is NULL or is an unchanged TOAST value
		// Does Postgres always prevent those cases?
		buf.WriteString(a.RowIdentityBefore[colIdx].V)
	}
	return buf.String()
}

func (a *RowAction) AfterActionRowIdentity() string {
	if !a.ChangesRowIdentity {
		return a.BeforeActionRowIdentity()
	}
	buf := bytes.Buffer{}
	for colIdx := range a.IdentityColumnIndices() {
		// TODO: possible bug if RowIdentityBefore is NULL or is an unchanged TOAST value
		// Does Postgres always prevent those cases?
		val := a.NewValues[colIdx]
		if val.Unchanged {
			buf.WriteString(a.RowIdentityBefore[colIdx].V)
		} else {
			buf.WriteString(val.V)
		}
	}
	return buf.String()
}
