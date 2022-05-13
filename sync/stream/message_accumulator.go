package stream

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/samjbobb/mammoth/sync/db"
	"github.com/sirupsen/logrus"
)

var (
	ErrRelationChanged    = errors.New("relation changed")
	ErrRelationNotFound   = errors.New("relation not found")
	ErrNoIdentityColumns  = errors.New("relation has no identity columns")
	ErrMessageLost        = errors.New("unexpected message order (messages are lost)")
	ErrUnknownMessageType = errors.New("unknown message type")
)

type Accumulator struct {
	tx          *db.WalTransaction
	typeIsArray map[uint32]bool
	// Maps arbitrary relation ID to Relation described in Relation messages.
	// > Every DML message contains an arbitrary relation ID, which can be mapped to an ID in the Relation messages.
	// > The Relation messages describe the schema of the given relation.
	// > The Relation message is sent for a given relation either because it is the first time we send a DML message
	// > for given relation in the current session or because the relation definition has changed since the last
	// > Relation message was sent for it.
	// > The protocol assumes that the client is capable of caching the metadata for as many relations as needed.
	// - https://www.postgresql.org/docs/10/protocol-logical-replication.html
	relationStore map[uint32]*db.Relation
}

func NewAccumulator(typeIsArray map[uint32]bool) *Accumulator {
	return &Accumulator{
		typeIsArray:   typeIsArray,
		relationStore: make(map[uint32]*db.Relation),
	}
}

// Add adds a logical replication message to the accumulator and returns a WalTransaction when the transaction is complete
func (wta *Accumulator) Add(msg pglogrepl.Message) (*db.WalTransaction, error) {
	switch v := msg.(type) {
	case *pglogrepl.BeginMessage:
		logrus.
			WithFields(
				logrus.Fields{
					"lsn": v.FinalLSN.String(),
					"xid": v.Xid,
				}).
			Debugln("receive begin message")
		// Start a new transaction
		wta.tx = &db.WalTransaction{
			LSN:       v.FinalLSN,
			BeginTime: &v.CommitTime,
		}
		return nil, nil
	case *pglogrepl.CommitMessage:
		logrus.
			WithFields(
				logrus.Fields{
					"lsn":             v.CommitLSN.String(),
					"transaction_lsn": v.TransactionEndLSN.String(),
				}).
			Debugln("receive commit message")
		if wta.tx == nil {
			return nil, fmt.Errorf("commit: %w", ErrMessageLost)
		}
		if wta.tx.LSN > 0 && wta.tx.LSN != v.CommitLSN {
			return nil, fmt.Errorf("commit: %w", ErrMessageLost)
		}
		wta.tx.CommitTime = &v.CommitTime
		tx := wta.tx
		wta.tx = nil
		return tx, nil
	case *pglogrepl.OriginMessage:
		logrus.Debugln("receive origin message")
		return nil, nil
	case *pglogrepl.RelationMessage:
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": v.RelationID,
					"replica":     v.ReplicaIdentity,
				}).
			Debugln("receive relation message")
		if wta.tx == nil {
			return nil, fmt.Errorf("commit: %w", ErrMessageLost)
		}
		if wta.tx.LSN == 0 {
			return nil, fmt.Errorf("commit: %w", ErrMessageLost)
		}
		rd := &db.Relation{
			Schema: v.Namespace,
			Table:  v.RelationName,
		}

		for _, rf := range v.Columns {
			isArray, exists := wta.typeIsArray[rf.DataType]
			if !exists {
				return nil, fmt.Errorf("unexpected data type (has it changed since introspection?): %v", rf.DataType)
			}
			c := db.Column{
				Name:       rf.Name,
				ValueType:  rf.DataType,
				IsIdentity: rf.Flags == 1,
				IsArray:    isArray,
			}
			rd.Columns = append(rd.Columns, c)
		}
		if len(rd.IdentityColumns()) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrNoIdentityColumns, rd.Table)
		}
		// According to the docs, Postgres sends a relation message for a relation before the first DML message for
		// the relation is sent in the current session, or because the relation definition has changed.
		// Experimentally, Postgres also seems to occasionally send a relation message for some other reason, so
		// comparing the previous relation and the new relation is necessary.
		if prevRelation, exists := wta.relationStore[v.RelationID]; exists {
			if !prevRelation.Equal(rd) {
				return nil, ErrRelationChanged
			}
		}

		wta.relationStore[v.RelationID] = rd
		return nil, nil
	case *pglogrepl.TypeMessage:
		logrus.Debugln("receive type message")
		return nil, nil
	case *pglogrepl.InsertMessage:
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": v.RelationID,
				}).
			Debugln("receive insert message")
		if wta.tx == nil {
			return nil, fmt.Errorf("commit: %w", ErrMessageLost)
		}
		action, err := wta.newRowAction(
			v.RelationID,
			db.Insert,
			nil,
			v.Tuple,
			false,
		)
		if err != nil {
			return nil, fmt.Errorf("create action data: %w", err)
		}
		wta.tx.Actions = append(wta.tx.Actions, action)
		return nil, nil
	case *pglogrepl.UpdateMessage:
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": v.RelationID,
				}).
			Debugln("receive update message")
		if wta.tx == nil {
			return nil, fmt.Errorf("commit: %w", ErrMessageLost)
		}
		action, err := wta.newRowAction(
			v.RelationID,
			db.Update,
			v.OldTuple,
			v.NewTuple,
			v.OldTupleType == byte('K'),
		)
		if err != nil {
			return nil, fmt.Errorf("create action data: %w", err)
		}
		wta.tx.Actions = append(wta.tx.Actions, action)
		return nil, nil
	case *pglogrepl.DeleteMessage:
		logrus.
			WithFields(
				logrus.Fields{
					"relation_id": v.RelationID,
				}).
			Debugln("receive delete message")
		if wta.tx == nil {
			return nil, fmt.Errorf("commit: %w", ErrMessageLost)
		}
		// Postgres only populates the identity columns in delete actions
		// Other columns are nil
		action, err := wta.newRowAction(
			v.RelationID,
			db.Delete,
			v.OldTuple,
			nil,
			false,
		)
		if err != nil {
			return nil, fmt.Errorf("create action data: %w", err)
		}
		wta.tx.Actions = append(wta.tx.Actions, action)
		return nil, nil
	case *pglogrepl.TruncateMessage:
		return nil, fmt.Errorf("truncate message not implemented")
	default:
		return nil, fmt.Errorf("%w : %s", ErrUnknownMessageType, msg.Type())
	}
}

// newRowAction create action from WAL message data.
func (wta *Accumulator) newRowAction(
	relationID uint32,
	kind db.ActionKind,
	oldTuple *pglogrepl.TupleData,
	newTuple *pglogrepl.TupleData,
	updateChangedRowIdentity bool,
) (a *db.RowAction, err error) {
	relation, ok := wta.relationStore[relationID]
	if !ok {
		return a, ErrRelationNotFound
	}
	a = &db.RowAction{
		Relation: relation,
		Kind:     kind,
	}
	switch kind {
	case db.Insert:
		// Inserts leave ChangesRowIdentity false and RowIdentityBefore nil
		newValues, err := valuesFromTuple(newTuple)
		if err != nil {
			return nil, err
		}
		a.NewValues = newValues
		// A little slight-of-hand: set RowIdentityBefore to the values after insert
		// RowIdentityBefore seems like it should be null for INSERTS, but the mergeActions logic
		// is a lot simpler if we make this concession.
		a.RowIdentityBefore = make([]*db.Value, len(a.Columns))
		for colIdx := range a.IdentityColumnIndices() {
			a.RowIdentityBefore[colIdx] = a.NewValues[colIdx]
		}
		return a, nil
	case db.Delete:
		// Delete action includes row identity in Old Tuple
		oldValues, err := valuesFromTuple(oldTuple)
		if err != nil {
			return nil, err
		}
		a.RowIdentityBefore = oldValues
		return a, nil
	case db.Update:
		// Update Action where row identity changed includes row identity in Old Tuple
		if updateChangedRowIdentity {
			oldValues, err := valuesFromTuple(oldTuple)
			if err != nil {
				return nil, err
			}
			a.RowIdentityBefore = oldValues
			newValues, err := valuesFromTuple(newTuple)
			if err != nil {
				return nil, err
			}
			a.NewValues = newValues
			a.ChangesRowIdentity = true
			return a, nil
		} else {
			// Updates where row identity did not change include new values in New Tuple
			newValues, err := valuesFromTuple(newTuple)
			if err != nil {
				return nil, err
			}
			a.NewValues = newValues
			// Row identity did not change so set it from NewValues
			a.RowIdentityBefore = make([]*db.Value, len(a.Columns))
			for colIdx := range a.IdentityColumnIndices() {
				a.RowIdentityBefore[colIdx] = a.NewValues[colIdx]
			}
			return a, nil
		}
	default:
		panic("Impossible Action Kind")
	}
}

func valuesFromTuple(t *pglogrepl.TupleData) ([]*db.Value, error) {
	if t == nil {
		return nil, nil
	}
	values := make([]*db.Value, len(t.Columns))
	for colIdx, tupleCol := range t.Columns {
		val := &db.Value{}
		// DataType indicates how the data is stored.
		//	 Byte1('n') Identifies the data as NULL value.
		//	 Or
		//	 Byte1('u') Identifies unchanged TOASTed value (the actual value is not sent).
		//	 Or
		//	 Byte1('t') Identifies the data as text formatted value.
		if tupleCol.DataType == byte('u') {
			val.Unchanged = true
		} else if tupleCol.DataType == byte('n') {
			val.Null = true
		} else if tupleCol.DataType == byte('t') {
			val.V = string(tupleCol.Data)
		} else {
			return nil, fmt.Errorf("message type %v: %w", tupleCol.DataType, ErrUnknownMessageType)
		}
		values[colIdx] = val
	}
	return values, nil
}
