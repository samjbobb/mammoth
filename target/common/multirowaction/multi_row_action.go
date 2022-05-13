package multirowaction

import (
	"errors"
	"fmt"

	"github.com/samjbobb/mammoth/sync/db"
)

var ErrInvalidActionSequence = errors.New("invalid message sequence")

type RelationActions struct {
	*db.Relation
	Actions []*db.RowAction
}

// Grouper returns actions grouped by table
// The returned map is keyed by table.
// RowActions are returned in a map keyed on row identity.
// Redundant actions to the same row identity are aggregated into one equivalent action.
// The order of row actions within each table and group is not maintained (because row actions are stored in a map),
func Grouper(batch []*db.WalTransaction) ([]*RelationActions, error) {
	relationActions := actionsByRelation(batch)
	for _, ra := range relationActions {
		if deduplicatedActions, err := deduplicateActions(ra.Actions); err != nil {
			return nil, err
		} else {
			ra.Actions = deduplicatedActions
		}
	}
	return relationActions, nil
}

// actionsByRelation collects all actions in a batch and groups them by relation,
// maintaining the relative order of the actions within each relation group.
func actionsByRelation(batch []*db.WalTransaction) []*RelationActions {
	tableId := func(a *db.RowAction) string {
		return fmt.Sprintf("%s.%s", a.Schema, a.Table)
	}
	relationMap := make(map[string]*RelationActions)
	for _, walTransaction := range batch {
		for _, walAction := range walTransaction.Actions {
			tId := tableId(walAction)
			tableGroup, exists := relationMap[tId]
			if exists {
				// Add action to list
				tableGroup.Actions = append(tableGroup.Actions, walAction)
			} else {
				// Create a new group with first action
				relationMap[tId] = &RelationActions{
					Relation: walAction.Relation,
					Actions:  []*db.RowAction{walAction},
				}
			}
		}
	}
	// Return only the values (lists of actions)
	var out []*RelationActions
	for _, group := range relationMap {
		out = append(out, group)
	}
	return out
}

// deduplicateActions creates a reduced list of actions for a relation, eliminating actions that operate on the same
// row, see mergeActions for details.
// The order of actions in the input is important but the order of actions in the output is not important, because
// the actions are guaranteed to not interact (each operates on independent rows).
func deduplicateActions(in []*db.RowAction) ([]*db.RowAction, error) {
	rowMap := make(map[string]*db.RowAction)
	for _, action := range in {
		// Calculate the row identity before and after merge
		// If they change, then the old row needs to be removed from tableGroup.Actions and the new one added
		existingAction, exists := rowMap[action.BeforeActionRowIdentity()]
		if !exists {
			// Add first action for this row
			rowMap[action.AfterActionRowIdentity()] = action
			continue
		}
		// Merge new action to a action for this row
		merged, err := mergeActions(existingAction, action)
		if err != nil {
			return nil, err
		}
		if merged == nil {
			delete(rowMap, existingAction.AfterActionRowIdentity())
		} else {
			if merged.AfterActionRowIdentity() != existingAction.AfterActionRowIdentity() {
				delete(rowMap, existingAction.AfterActionRowIdentity())
			}
			rowMap[merged.AfterActionRowIdentity()] = merged
		}
	}

	// Return only the values (list of actions)
	var out []*db.RowAction
	for _, group := range rowMap {
		out = append(out, group)
	}
	return out, nil
}

// mergeActions combines two actions for the same row into one equivalent action
// the new action must come after the existing action in the event stream (ordering is important)
// (insert, insert) => not valid
// (insert, update) => insert
// (insert, delete) => no op (nil)
// (update, insert) => not valid
// (update, update) => update
// (update, delete) => delete
// (delete, insert) => update
// (delete, update) => not valid
// (delete, delete) => not valid
func mergeActions(a, b *db.RowAction) (*db.RowAction, error) {
	type p struct {
		a db.ActionKind
		b db.ActionKind
	}
	pair := p{a: a.Kind, b: b.Kind}
	switch pair {
	case p{a: db.Insert, b: db.Update}:
		newValues := db.Map(b.NewValues, func(colIdx int, value *db.Value) *db.Value {
			if b.NewValues[colIdx].Unchanged {
				return a.NewValues[colIdx]
			}
			return value
		})
		rowIdentity := db.Map(newValues, func(colIdx int, value *db.Value) *db.Value {
			if a.Columns[colIdx].IsIdentity {
				return value
			}
			return nil
		})
		return &db.RowAction{
			Relation:           a.Relation,
			Kind:               db.Insert,
			RowIdentityBefore:  rowIdentity,
			ChangesRowIdentity: false,
			// take values from the second action unless there are unchanged toast values
			NewValues: newValues,
		}, nil
	case p{a: db.Update, b: db.Update}:
		return &db.RowAction{
			Relation:           a.Relation,
			Kind:               a.Kind,
			RowIdentityBefore:  a.RowIdentityBefore,
			ChangesRowIdentity: a.ChangesRowIdentity || b.ChangesRowIdentity,
			// take values from the second action unless there are unchanged toast values
			NewValues: db.Map(b.NewValues, func(colIdx int, value *db.Value) *db.Value {
				if b.NewValues[colIdx].Unchanged {
					return a.NewValues[colIdx]
				}
				return value
			}),
		}, nil
	case p{a: db.Insert, b: db.Delete}:
		return nil, nil
	case p{a: db.Update, b: db.Delete}:
		if a.ChangesRowIdentity {
			return &db.RowAction{
				Relation:           a.Relation,
				Kind:               db.Delete,
				ChangesRowIdentity: false,
				RowIdentityBefore:  a.RowIdentityBefore,
				NewValues:          nil,
			}, nil
		}
		return b, nil
	case p{a: db.Delete, b: db.Insert}:
		return &db.RowAction{
			Relation:          a.Relation,
			Kind:              db.Update,
			RowIdentityBefore: a.RowIdentityBefore,
			NewValues:         b.NewValues,
		}, nil
	default:
		// (Insert, Insert), (Update, Insert), (Delete, Update), (Delete, Delete)
		return nil, ErrInvalidActionSequence
	}
}
