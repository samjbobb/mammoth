package multirowaction

import (
	"testing"

	"github.com/samjbobb/mammoth/sync/db"
	"github.com/stretchr/testify/assert"
)

func Test_Grouper(t *testing.T) {
	rel1 := &db.Relation{
		Schema: "schema",
		Table:  "table1",
		Columns: []db.Column{
			{
				Name:       "id",
				ValueType:  0,
				IsIdentity: true,
			},
			{
				Name:       "name",
				ValueType:  0,
				IsIdentity: false,
			},
		},
	}

	rel2 := &db.Relation{
		Schema: "schema",
		Table:  "table2",
		Columns: []db.Column{
			{
				Name:       "id",
				ValueType:  0,
				IsIdentity: true,
			},
		},
	}

	type args struct {
		batch []*db.WalTransaction
	}
	tests := []struct {
		name    string
		args    args
		want    []*RelationActions
		wantErr bool
	}{
		{
			name: "groups actions with multiple tables and row identity changes",
			args: args{
				batch: []*db.WalTransaction{{
					Actions: []*db.RowAction{
						{
							Kind:              "INSERT",
							Relation:          rel1,
							RowIdentityBefore: []*db.Value{{V: "1"}, nil},
							NewValues:         []*db.Value{{V: "1"}, {V: "sam"}},
						},
						{
							Kind:              "DELETE",
							Relation:          rel2,
							RowIdentityBefore: []*db.Value{{V: "100"}},
						},
						{
							Kind:              "INSERT",
							Relation:          rel1,
							RowIdentityBefore: []*db.Value{{V: "2"}, nil},
							NewValues:         []*db.Value{{V: "2"}, {V: "gus"}},
						},
						{
							Kind:              "UPDATE",
							Relation:          rel1,
							RowIdentityBefore: []*db.Value{{V: "1"}, nil},
							NewValues:         []*db.Value{{V: "1"}, {V: "sam bobb"}},
						},
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "1"}, nil},
							NewValues:          []*db.Value{{V: "3"}, {V: "", Unchanged: true}},
							ChangesRowIdentity: true,
						},
						{
							Kind:              "UPDATE",
							Relation:          rel1,
							RowIdentityBefore: []*db.Value{{V: "3"}, nil},
							NewValues:         []*db.Value{{V: "3"}, {V: "Sam Bobb"}},
						},
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "gus the dog"}},
							ChangesRowIdentity: false,
						},
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "Gus Gus Gus"}},
							ChangesRowIdentity: false,
						},
					},
					LSN: 0,
				}}},
			want: []*RelationActions{
				{
					Relation: rel1,
					Actions: []*db.RowAction{
						{
							Relation:          rel1,
							Kind:              "INSERT",
							RowIdentityBefore: []*db.Value{{V: "3"}, nil},
							NewValues:         []*db.Value{{V: "3"}, {V: "Sam Bobb"}},
						},
						{
							Relation:          rel1,
							Kind:              "INSERT",
							RowIdentityBefore: []*db.Value{{V: "2"}, nil},
							NewValues:         []*db.Value{{V: "2"}, {V: "gus"}},
						},
						{
							Relation:          rel1,
							Kind:              "UPDATE",
							RowIdentityBefore: []*db.Value{{V: "10"}, nil},
							NewValues:         []*db.Value{{V: "10"}, {V: "Gus Gus Gus"}},
						},
					},
				},
				{
					Relation: rel2,
					Actions: []*db.RowAction{
						{
							Kind:              "DELETE",
							Relation:          rel2,
							RowIdentityBefore: []*db.Value{{V: "100"}},
						},
					},
				}},
			wantErr: false,
		},
		{
			name: "groups multiple updates on same row with unchanged values",
			args: args{
				batch: []*db.WalTransaction{{
					Actions: []*db.RowAction{
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "gus the dog"}},
							ChangesRowIdentity: false,
						},
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "Gus Gus Gus"}},
							ChangesRowIdentity: false,
						},
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "", Unchanged: true}},
							ChangesRowIdentity: false,
						},
					},
					LSN: 0,
				}}},
			want: []*RelationActions{
				{
					Relation: rel1,
					Actions: []*db.RowAction{
						{
							Relation:          rel1,
							Kind:              "UPDATE",
							RowIdentityBefore: []*db.Value{{V: "10"}, nil},
							NewValues:         []*db.Value{{V: "10"}, {V: "Gus Gus Gus"}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "groups insert followed by delete as no op",
			args: args{
				batch: []*db.WalTransaction{{
					Actions: []*db.RowAction{
						{
							Kind:               "INSERT",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "gus the dog"}},
							ChangesRowIdentity: false,
						},
						{
							Kind:               "DELETE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          nil,
							ChangesRowIdentity: false,
						},
					},
					LSN: 0,
				}}},
			want: []*RelationActions{
				{
					Relation: rel1,
					Actions:  nil,
				},
			},
			wantErr: false,
		},
		{
			name: "groups update with identity change followed by delete as delete on first identity",
			args: args{
				batch: []*db.WalTransaction{{
					Actions: []*db.RowAction{
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "20"}, {V: "gus the dog"}},
							ChangesRowIdentity: true,
						},
						{
							Kind:               "DELETE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "20"}, nil},
							NewValues:          nil,
							ChangesRowIdentity: false,
						},
					},
					LSN: 0,
				}}},
			want: []*RelationActions{
				{
					Relation: rel1,
					Actions: []*db.RowAction{
						{
							Relation:          rel1,
							Kind:              "DELETE",
							RowIdentityBefore: []*db.Value{{V: "10"}, nil},
							NewValues:         nil,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "groups update followed by delete as delete",
			args: args{
				batch: []*db.WalTransaction{{
					Actions: []*db.RowAction{
						{
							Kind:               "UPDATE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "gus the dog"}},
							ChangesRowIdentity: false,
						},
						{
							Kind:               "DELETE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          nil,
							ChangesRowIdentity: false,
						},
					},
					LSN: 0,
				}}},
			want: []*RelationActions{
				{
					Relation: rel1,
					Actions: []*db.RowAction{
						{
							Relation:          rel1,
							Kind:              "DELETE",
							RowIdentityBefore: []*db.Value{{V: "10"}, nil},
							NewValues:         nil,
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "groups delete followed by insert as an update",
			args: args{
				batch: []*db.WalTransaction{{
					Actions: []*db.RowAction{
						{
							Kind:               "DELETE",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          nil,
							ChangesRowIdentity: false,
						},
						{
							Kind:               "INSERT",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "Gus Gus Gus"}},
							ChangesRowIdentity: false,
						},
					},
					LSN: 0,
				}}},
			want: []*RelationActions{
				{
					Relation: rel1,
					Actions: []*db.RowAction{
						{
							Relation:          rel1,
							Kind:              "UPDATE",
							RowIdentityBefore: []*db.Value{{V: "10"}, nil},
							NewValues:         []*db.Value{{V: "10"}, {V: "Gus Gus Gus"}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "returns error for invalid action sequence",
			args: args{
				batch: []*db.WalTransaction{{
					Actions: []*db.RowAction{
						{
							Kind:               "INSERT",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "Gus Gus Gus"}},
							ChangesRowIdentity: false,
						},
						{
							Kind:               "INSERT",
							Relation:           rel1,
							RowIdentityBefore:  []*db.Value{{V: "10"}, nil},
							NewValues:          []*db.Value{{V: "10"}, {V: "Gus Gus Gus 2"}},
							ChangesRowIdentity: false,
						},
					},
					LSN: 0,
				}}},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Grouper(tt.args.batch)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, got, len(tt.want))

			for _, relWant := range tt.want {
				found := 0
				for _, relGot := range got {
					if relWant.Schema == relGot.Schema && relWant.Relation.Table == relGot.Relation.Table {
						assert.ElementsMatch(t, relWant.Actions, relWant.Actions)
						found += 1
					}
				}
				assert.Equal(t, 1, found)
			}
		})
	}
}
