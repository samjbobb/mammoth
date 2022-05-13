package stream

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/samjbobb/mammoth/sync/db"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var typeIsArrayMap = map[uint32]bool{
	pgtype.TextOID: false,
	pgtype.Int4OID: false,
}

func TestWalTransactionAccumulator_Add(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		testBody func()
	}{
		{
			name: "successful insert",
			testBody: func() {
				wta := NewAccumulator(typeIsArrayMap)
				{
					tx, err := wta.Add(&pglogrepl.BeginMessage{
						FinalLSN:   10,
						CommitTime: now,
						Xid:        0,
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				{
					tx, err := wta.Add(&pglogrepl.RelationMessage{
						RelationID:      1000,
						Namespace:       "schema_a",
						RelationName:    "table_1",
						ReplicaIdentity: 1,
						ColumnNum:       2,
						Columns: []*pglogrepl.RelationMessageColumn{
							// Flags = 1 marks the column as included in the primary key
							{Flags: 1, Name: "fruit", DataType: pgtype.TextOID, TypeModifier: 0},
							{Flags: 0, Name: "plant", DataType: pgtype.TextOID, TypeModifier: 0},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				// Same relation message more that once has no effect
				{
					tx, err := wta.Add(&pglogrepl.RelationMessage{
						RelationID:      1000,
						Namespace:       "schema_a",
						RelationName:    "table_1",
						ReplicaIdentity: 1,
						ColumnNum:       2,
						Columns: []*pglogrepl.RelationMessageColumn{
							// Flags = 1 marks the column as included in the primary key
							{Flags: 1, Name: "fruit", DataType: pgtype.TextOID, TypeModifier: 0},
							{Flags: 0, Name: "plant", DataType: pgtype.TextOID, TypeModifier: 0},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				{
					tx, err := wta.Add(&pglogrepl.InsertMessage{
						RelationID: 1000,
						Tuple: &pglogrepl.TupleData{
							ColumnNum: 2,
							Columns: []*pglogrepl.TupleDataColumn{
								{DataType: byte('t'), Length: 9, Data: []byte("Blueberry")},
								{DataType: byte('t'), Length: 4, Data: []byte("Bush")},
							},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				// Another insert in the same relation
				{
					tx, err := wta.Add(&pglogrepl.InsertMessage{
						RelationID: 1000,
						Tuple: &pglogrepl.TupleData{
							ColumnNum: 2,
							Columns: []*pglogrepl.TupleDataColumn{
								{DataType: byte('t'), Length: 5, Data: []byte("Apple")},
								{DataType: byte('t'), Length: 4, Data: []byte("Tree")},
							},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				// It's possible to have multiple relations in the same transaction
				{
					tx, err := wta.Add(&pglogrepl.RelationMessage{
						RelationID:      2000,
						Namespace:       "schema_a",
						RelationName:    "table_2",
						ReplicaIdentity: 1,
						ColumnNum:       2,
						Columns: []*pglogrepl.RelationMessageColumn{
							// Flags = 1 marks the column as included in the primary key
							{Flags: 1, Name: "animal", DataType: pgtype.TextOID, TypeModifier: 0},
							{Flags: 0, Name: "legs", DataType: pgtype.Int4OID, TypeModifier: 0},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				// Update message on the second table
				{
					tx, err := wta.Add(&pglogrepl.UpdateMessage{
						RelationID: 2000,
						// OldTupleType == byte('K') indicates that the primary key of the row has changed
						// previous primary key values are included in OldTuple
						OldTupleType: byte('K'),
						// UpdateMessage provides OldTuple and NewTuple.
						OldTuple: &pglogrepl.TupleData{
							ColumnNum: 2,
							Columns: []*pglogrepl.TupleDataColumn{
								{DataType: byte('t'), Length: 5, Data: []byte("Old Horse")},
								{DataType: byte('t'), Length: 1, Data: []byte("4")},
							},
						},
						NewTuple: &pglogrepl.TupleData{
							ColumnNum: 2,
							Columns: []*pglogrepl.TupleDataColumn{
								{DataType: byte('t'), Length: 5, Data: []byte("Horse")},
								{DataType: byte('t'), Length: 1, Data: []byte("4")},
							},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				// Delete message on the second table
				{
					tx, err := wta.Add(&pglogrepl.DeleteMessage{
						RelationID: 2000,
						OldTuple: &pglogrepl.TupleData{
							ColumnNum: 2,
							// DeleteMessage only provides data for key columns
							Columns: []*pglogrepl.TupleDataColumn{
								{DataType: byte('t'), Length: 3, Data: []byte("Ant")},
								// byte('n') indicates a null value
								{DataType: byte('n'), Length: 0, Data: []byte{}},
							},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				{
					tx, err := wta.Add(&pglogrepl.CommitMessage{
						Flags:             0,
						CommitLSN:         10,
						TransactionEndLSN: 10,
						CommitTime:        now,
					})
					assert.Nil(t, err)
					assert.Nil(t, wta.tx)
					assert.Equal(t, &db.WalTransaction{
						LSN:        10,
						BeginTime:  &now,
						CommitTime: &now,
						Actions: []*db.RowAction{
							{
								Relation: &db.Relation{
									Schema: "schema_a",
									Table:  "table_1",
									Columns: []db.Column{
										{Name: "fruit", ValueType: pgtype.TextOID, IsIdentity: true},
										{Name: "plant", ValueType: pgtype.TextOID, IsIdentity: false},
									},
								},
								Kind:              db.Insert,
								NewValues:         []*db.Value{{V: "Blueberry"}, {V: "Bush"}},
								RowIdentityBefore: []*db.Value{{V: "Blueberry"}, nil},
							},
							{
								Relation: &db.Relation{
									Schema: "schema_a",
									Table:  "table_1",
									Columns: []db.Column{
										{Name: "fruit", ValueType: pgtype.TextOID, IsIdentity: true},
										{Name: "plant", ValueType: pgtype.TextOID, IsIdentity: false},
									},
								},
								Kind:              db.Insert,
								NewValues:         []*db.Value{{V: "Apple"}, {V: "Tree"}},
								RowIdentityBefore: []*db.Value{{V: "Apple"}, nil},
							},
							{
								Relation: &db.Relation{
									Schema: "schema_a",
									Table:  "table_2",
									Columns: []db.Column{
										{Name: "animal", ValueType: pgtype.TextOID, IsIdentity: true},
										{Name: "legs", ValueType: pgtype.Int4OID, IsIdentity: false},
									},
								},
								Kind:               db.Update,
								ChangesRowIdentity: true,
								RowIdentityBefore:  []*db.Value{{V: "Old Horse"}, {V: "4"}},
								NewValues:          []*db.Value{{V: "Horse"}, {V: "4"}},
							},
							{
								Relation: &db.Relation{
									Schema: "schema_a",
									Table:  "table_2",
									Columns: []db.Column{
										{Name: "animal", ValueType: pgtype.TextOID, IsIdentity: true},
										{Name: "legs", ValueType: pgtype.Int4OID, IsIdentity: false},
									},
								},
								Kind:              db.Delete,
								RowIdentityBefore: []*db.Value{{V: "Ant"}, {V: "", Null: true}},
							},
						},
					}, tx)
				}
			},
		},
		{
			name: "error: relation changed",
			testBody: func() {
				wta := NewAccumulator(typeIsArrayMap)
				{
					tx, err := wta.Add(&pglogrepl.BeginMessage{
						FinalLSN:   10,
						CommitTime: now,
						Xid:        0,
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				{
					tx, err := wta.Add(&pglogrepl.RelationMessage{
						RelationID:      1000,
						Namespace:       "schema_a",
						RelationName:    "table_1",
						ReplicaIdentity: 1,
						ColumnNum:       2,
						Columns: []*pglogrepl.RelationMessageColumn{
							// Flags = 1 marks the column as included in the primary key
							{Flags: 1, Name: "fruit", DataType: pgtype.TextOID, TypeModifier: 0},
							{Flags: 0, Name: "plant", DataType: pgtype.TextOID, TypeModifier: 0},
						},
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				// Relation changes:
				{
					tx, err := wta.Add(&pglogrepl.RelationMessage{
						RelationID:      1000,
						Namespace:       "schema_a",
						RelationName:    "table_1",
						ReplicaIdentity: 1,
						ColumnNum:       2,
						Columns: []*pglogrepl.RelationMessageColumn{
							// Flags = 1 marks the column as included in the primary key
							{Flags: 1, Name: "DIFFERENT_NAME", DataType: pgtype.TextOID, TypeModifier: 0},
							{Flags: 0, Name: "plant", DataType: pgtype.TextOID, TypeModifier: 0},
						},
					})
					assert.ErrorIs(t, err, ErrRelationChanged)
					assert.Nil(t, tx)
				}
			},
		},
		{
			name: "error: insert message before relation message",
			testBody: func() {
				wta := NewAccumulator(typeIsArrayMap)
				{
					tx, err := wta.Add(&pglogrepl.BeginMessage{
						FinalLSN:   10,
						CommitTime: now,
						Xid:        0,
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				{
					tx, err := wta.Add(&pglogrepl.InsertMessage{
						RelationID: 1000,
						Tuple: &pglogrepl.TupleData{
							ColumnNum: 2,
							Columns: []*pglogrepl.TupleDataColumn{
								{DataType: byte('t'), Length: 9, Data: []byte("Blueberry")},
								{DataType: byte('t'), Length: 4, Data: []byte("Bush")},
							},
						},
					})
					assert.ErrorIs(t, err, ErrRelationNotFound)
					assert.Nil(t, tx)
				}
			},
		},
		{
			name: "error: relation with no key columns",
			testBody: func() {
				wta := NewAccumulator(typeIsArrayMap)
				{
					tx, err := wta.Add(&pglogrepl.BeginMessage{
						FinalLSN:   10,
						CommitTime: now,
						Xid:        0,
					})
					assert.Nil(t, err)
					assert.Nil(t, tx)
				}
				{
					tx, err := wta.Add(&pglogrepl.RelationMessage{
						RelationID:      1000,
						Namespace:       "schema_a",
						RelationName:    "table_1",
						ReplicaIdentity: 1,
						ColumnNum:       2,
						Columns: []*pglogrepl.RelationMessageColumn{
							// Flags = 1 marks the column as included in the primary key
							{Flags: 0, Name: "fruit", DataType: pgtype.TextOID, TypeModifier: 0},
							{Flags: 0, Name: "plant", DataType: pgtype.TextOID, TypeModifier: 0},
						},
					})
					assert.ErrorIs(t, err, ErrNoIdentityColumns)
					assert.Nil(t, tx)
				}
			},
		},
		{
			name: "out of order messages: no begin message",
			testBody: func() {
				wta := NewAccumulator(typeIsArrayMap)
				{
					tx, err := wta.Add(&pglogrepl.RelationMessage{
						RelationID:      1000,
						Namespace:       "schema_a",
						RelationName:    "table_1",
						ReplicaIdentity: 1,
						ColumnNum:       2,
						Columns: []*pglogrepl.RelationMessageColumn{
							// Flags = 1 marks the column as included in the primary key
							{Flags: 0, Name: "fruit", DataType: pgtype.TextOID, TypeModifier: 0},
							{Flags: 0, Name: "plant", DataType: pgtype.TextOID, TypeModifier: 0},
						},
					})
					assert.ErrorIs(t, err, ErrMessageLost)
					assert.Nil(t, tx)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testBody()
		})
	}
}
