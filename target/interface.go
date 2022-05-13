package target

import (
	"context"
	"io"

	"github.com/samjbobb/mammoth/sync/db"
)

// Postgres delivers the text representation of values as a CSV stream `r` in InitializeRelation
// and in the `batch` in Write.
// The column types are described by the accompanying db.Relation
// It's the responsibility of the target to transform these into a format suitable for the destination
// This may involve handling numbers, bools, text, and JSON, as well as the Postgres Array format {...}

type TargetInterface interface {
	// InitializeRelation takes a relation definition and a CSV file as a reader and
	// creates the relation in the target and populates it with data from the CSV
	InitializeRelation(ctx context.Context, relation *db.Relation, r io.Reader) error
	// VerifyRelation takes a relation definition and checks that the relation in the target
	// matches the schema
	VerifyRelation(ctx context.Context, relation *db.Relation) (bool, error)
	// Write accepts a batch of WAL transactions and writes them to the target
	Write(ctx context.Context, batch []*db.WalTransaction) error
	// Close does any needed cleanup and closes any open connections
	Close(ctx context.Context) error
}
