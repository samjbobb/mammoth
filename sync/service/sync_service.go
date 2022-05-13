package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/samjbobb/mammoth/config"
	"github.com/samjbobb/mammoth/sync/db"
	"github.com/samjbobb/mammoth/sync/stream"
	"github.com/samjbobb/mammoth/target"
	"github.com/samjbobb/mammoth/utils"
	"github.com/sirupsen/logrus"
)

// service is the main sync operator
type service struct {
	cfg    *config.Config
	target target.TargetInterface
	// TODO: replace this conn with a interface so it can be mocked
	pool          *pgxpool.Pool
	relationNames []pgx.Identifier
	streamer      *stream.Stream
}

// NewService create and initialize new service instance.
func NewService(
	cfg *config.Config,
	pgxPool *pgxpool.Pool,
	replConn *pgconn.PgConn,
	target target.TargetInterface,
) (*service, error) {
	relationNames := make([]pgx.Identifier, len(cfg.Sync.Tables))
	for idx, table := range cfg.Sync.Tables {
		relationNames[idx] = strings.Split(table, ".")
	}
	if len(relationNames) == 0 {
		return nil, errors.New("must configure at least one table to sync")
	}

	s := &service{
		cfg:           cfg,
		target:        target,
		pool:          pgxPool,
		relationNames: relationNames,
		streamer:      stream.NewStream(pgxPool, replConn, target, cfg.Postgres.SlotName, cfg.Sync.BatchMaxItems, cfg.Sync.BatchTimeout, cfg.Sync.SkipAcknowledge),
	}

	return s, nil
}

// Run is the main entry point
func (s *service) Run(ctx context.Context) error {
	logrus.Infoln("service was started")
	if publicationMatches, err := s.publicationMatchesConfig(ctx); err != nil {
		return fmt.Errorf("could not check publication: %w", err)
	} else if !publicationMatches {
		logrus.Infoln("publication out of sync with configured relations to sync. Recreating publication and replication slot.")
		if err := s.dropReplicationSlot(ctx); err != nil {
			return fmt.Errorf("could not drop replication slot: %w", err)
		}
		if err := s.createPublication(ctx); err != nil {
			return fmt.Errorf("could not create publication: %w", err)
		}
	} else {
		logrus.Infoln("publication in sync with configured relations")
	}

	if exists, restartLsn, err := s.replicationSlotStatus(ctx); err != nil {
		return fmt.Errorf("could not check slot: %w", err)
	} else if !exists {
		logrus.Infoln("replication slot does not exist. Creating.")
		snapshotName, err := s.streamer.CreateSlot(ctx)
		if err != nil {
			logrus.Fatalln(err)
		}
		// Sync initial snapshot from postgres to the target
		if err := s.snapshotToTarget(ctx, snapshotName); err != nil {
			return fmt.Errorf("could not snapshot: %w", err)
		}
	} else {
		logrus.Infoln("replication slot found. Restarting from:", restartLsn)
		s.streamer.SetRestartLSN(restartLsn)
	}

	if relationsMatch, err := s.verifyRelations(ctx); err != nil {
		return fmt.Errorf("could not verify relations: %w", err)
	} else if !relationsMatch {
		// Relation schema has deviated, drop replication slot, and return
		// with ErrRelationChanged.
		// Replication connection must be closed to drop replication slot
		if err := s.streamer.Close(ctx); err != nil {
			return err
		}
		if err := s.dropReplicationSlot(ctx); err != nil {
			return fmt.Errorf("could not delete replication slot: %w", err)
		}
		if err := s.Stop(ctx); err != nil {
			return fmt.Errorf("could not close connections: %w", err)
		}
		return stream.ErrRelationChanged
	}

	if err := s.streamer.Stream(ctx); err == stream.ErrRelationChanged {
		// Relation schema has deviated, drop replication slot, and return
		// with ErrRelationChanged.
		// Replication connection must be closed to drop replication slot
		if err := s.streamer.Close(ctx); err != nil {
			return err
		}
		if err := s.dropReplicationSlot(ctx); err != nil {
			return fmt.Errorf("could not delete replication slot: %w", err)
		}
		if err := s.Stop(ctx); err != nil {
			return fmt.Errorf("could not close connections: %w", err)
		}
		return err
	} else if err != nil {
		return fmt.Errorf("unhandled error during streaming: %w", err)
	}
	return nil
}

// verifyRelations checks that all tables configured for syncing have matching schema between
// Postgres and the target.
func (s *service) verifyRelations(ctx context.Context) (bool, error) {
	logrus.Infoln("verify relations")
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() {
		utils.PanicIfErr(tx.Commit(ctx))
	}()
	for _, relationName := range s.relationNames {
		logrus.WithField("table", relationName.Sanitize()).Infoln("verify")
		rel, err := s.introspectRelation(ctx, tx, relationName)
		if err != nil {
			return false, fmt.Errorf("could not introspect relation %v: %w", relationName, err)
		}
		if ok, err := s.target.VerifyRelation(ctx, rel); err != nil {
			return false, fmt.Errorf("could not verify relation: %w", err)
		} else if !ok {
			logrus.WithField("table", relationName.Sanitize()).Infoln("schema mismatch between postgres and target")
			return false, nil
		}
	}
	logrus.Infoln("finished verify relations")
	return true, nil
}

func (s *service) snapshotToTarget(ctx context.Context, consistentSnapshotName string) error {
	// from: https://github.com/hasura/pgdeltastream/blob/master/db/snapshot.go#L14
	logrus.Info("Begin transaction")
	snapshotTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return fmt.Errorf("could not begin snapshot tx: %w", err)
	}

	defer func() {
		logrus.Debugln("Committing snapshot transaction")
		if err := snapshotTx.Commit(ctx); err != nil {
			logrus.WithError(err).Warnln("could not commit transaction")
		}
		logrus.Debugln("Snapshot transaction committed")
	}()

	logrus.Info("Setting transaction snapshot ", consistentSnapshotName)
	_, err = snapshotTx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", consistentSnapshotName))
	if err != nil {
		return fmt.Errorf("could not set consistent snapshot name: %w", err)
	}

	// It might seem like there is an opportunity to process tables concurrently,
	// but each requires a query to Postgres within the same transaction (in order
	// to make a consistent snapshot).
	var wg sync.WaitGroup
	wg.Add(len(s.relationNames))
	for _, relationName := range s.relationNames {
		logrus.Infoln("Starting snapshot for relation:", relationName.Sanitize())
		rel, err := s.introspectRelation(ctx, snapshotTx, relationName)
		if err != nil {
			return fmt.Errorf("could not introspect relation %v: %w", relationName, err)
		}

		reader, writer := io.Pipe()

		// Start goroutine where the target will read from the pipe
		go func() {
			if err := s.target.InitializeRelation(ctx, rel, reader); err != nil {
				// TODO: better error handling here
				panic(fmt.Errorf("could not write snapshot: %w", err))
			}
			wg.Done()
		}()

		// Write to the pipe from postgres
		if _, err := snapshotTx.Conn().PgConn().CopyTo(ctx, writer, fmt.Sprintf("copy %s to stdout with (format csv, header true)", relationName.Sanitize())); err != nil {
			return fmt.Errorf("could not copy to writer: %w", err)
		}

		// Close the pipe, which causes the consumer to finish
		if err := writer.Close(); err != nil {
			return fmt.Errorf("could not close writer: %w", err)
		}
		logrus.Infoln("Completed snapshot for relation:", relationName.Sanitize())

	}
	wg.Wait()

	return nil
}

// Stop closes functions and finishes
func (s *service) Stop(ctx context.Context) error {
	var err error
	err = s.target.Close(ctx)
	if err != nil {
		return err
	}
	s.pool.Close()
	err = s.streamer.Close(ctx)
	if err != nil {
		return err
	}
	logrus.Infoln("service was stopped")
	return nil
}

func (s *service) replicationSlotStatus(ctx context.Context) (bool, pglogrepl.LSN, error) {
	var result string
	if err := s.pool.QueryRow(
		ctx,
		"select restart_lsn from pg_replication_slots where slot_name = $1;",
		s.cfg.Postgres.SlotName).Scan(&result); err == pgx.ErrNoRows {
		return false, 0, nil
	} else if err != nil {
		return false, 0, err
	}
	restartLSN, err := pglogrepl.ParseLSN(result)
	if err != nil {
		return true, 0, err
	}
	return true, restartLSN, nil
}

func (s *service) publicationMatchesConfig(ctx context.Context) (bool, error) {
	rows, err := s.pool.Query(ctx, `select * from pg_publication_tables where pubname = $1;`, s.cfg.Postgres.SlotName)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	type publicationTable struct {
		pubname    string
		schemaname string
		tablename  string
	}
	publicationTables := make([]*publicationTable, 0)
	for rows.Next() {
		var pTable publicationTable
		if err := rows.Scan(&pTable.pubname, &pTable.schemaname, &pTable.tablename); err != nil {
			return false, fmt.Errorf("scan rows: %w", err)
		}
		publicationTables = append(publicationTables, &pTable)
	}
	if len(publicationTables) == 0 {
		return false, nil
	}
	if len(publicationTables) != len(s.relationNames) {
		return false, nil
	}
RelationsLoop:
	for _, relation := range s.relationNames {
		for _, pubTable := range publicationTables {
			if relation[0] == pubTable.schemaname && relation[1] == pubTable.tablename {
				continue RelationsLoop
			}
		}
		return false, nil
	}
	return true, nil
}

// createPublication drops and recreates publication for given relations
func (s *service) createPublication(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`drop publication if exists %s;`, s.cfg.Postgres.SlotName))
	if err != nil {
		return err
	}
	tables := make([]string, len(s.relationNames))
	for idx, relation := range s.relationNames {
		tables[idx] = relation.Sanitize()
	}
	_, err = s.pool.Exec(ctx, fmt.Sprintf(`create publication "%s" for table only %s`, s.cfg.Postgres.SlotName, strings.Join(tables, ", ")))
	return err
}

func (s *service) dropReplicationSlot(ctx context.Context) error {
	if s.cfg.Sync.ProhibitDropSlot {
		return fmt.Errorf("would drop replication slot but prevented by config")
	}
	if exists, _, err := s.replicationSlotStatus(ctx); err != nil {
		return err
	} else if !exists {
		return nil
	}
	_, err := s.pool.Exec(ctx, "select pg_drop_replication_slot($1)", s.cfg.Postgres.SlotName)
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		return nil
	}
	return err
}

func (s *service) introspectRelation(ctx context.Context, tx pgx.Tx, relationName pgx.Identifier) (*db.Relation, error) {
	// Based on https://github.com/transferwise/pipelinewise-tap-postgres/blob/ae9b211a9302c3ded42bcbfa06deccefc215dc07/tap_postgres/discovery_utils.py#L52
	// This query gets data that is currently not used and gets all columns for all tables at once
	// Keeping this around in case we want to refactor to all-at-once instead of table-by-table introspection.
	const query = `
	with r as (
		select n.nspname                                                                                                                 as schema_name,
			   pg_class.relname                                                                                                            as table_name,
			   a.attname                                                                                                                   as column_name,
			   a.attnum                                                                                                                    as ordinal_position,
			   coalesce(i.indisprimary, false)                                                                                             as primary_key,
			   a.atttypid                                                                                                                  as data_type_oid,
			   format_type(a.atttypid, null::integer)                                                                                      as data_type_name,
			   information_schema._pg_char_max_length(case
														  when coalesce(subpgt.typtype, pgt.typtype) = 'd'
															  then coalesce(subpgt.typbasetype, pgt.typbasetype)
														  else coalesce(subpgt.oid, pgt.oid)
														  end,
													  information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number   as character_maximum_length,
			   information_schema._pg_numeric_precision(case
															when coalesce(subpgt.typtype, pgt.typtype) = 'd'
																then coalesce(subpgt.typbasetype, pgt.typbasetype)
															else coalesce(subpgt.oid, pgt.oid)
															end,
														information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number as numeric_precision,
			   information_schema._pg_numeric_scale(case
														when coalesce(subpgt.typtype, pgt.typtype) = 'd'
															then coalesce(subpgt.typbasetype, pgt.typbasetype)
														else coalesce(subpgt.oid, pgt.oid)
														end,
													information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number     as numeric_scale,
			   pgt.typcategory = 'A'                                                                                                       as is_array,
			   coalesce(subpgt.typtype, pgt.typtype) = 'e'                                                                                 as is_enum
		from pg_attribute a
				 left join pg_type as pgt on a.atttypid = pgt.oid
				 join pg_class
					  on pg_class.oid = a.attrelid
				 join pg_catalog.pg_namespace n
					  on n.oid = pg_class.relnamespace
				 left outer join pg_index as i
								 on a.attrelid = i.indrelid
									 and a.attnum = any (i.indkey)
									 and i.indisprimary = true
				 left outer join pg_type as subpgt
								 on pgt.typelem = subpgt.oid
									 and pgt.typelem != 0
		where a.attnum > 0
		  and not a.attisdropped
		  and pg_class.relkind in ('r', 'p')
		  and n.nspname not in ('pg_toast', 'pg_catalog', 'information_schema')
		  and has_column_privilege(pg_class.oid, a.attname, 'SELECT') = true
	)
	select column_name, primary_key, data_type_oid, is_array
	from r
	where schema_name = $1
	  and table_name = $2
	order by ordinal_position;`

	rows, err := tx.Query(ctx, query, relationName[0], relationName[1])
	if err != nil {
		return nil, fmt.Errorf("introspection: %w", err)
	}
	defer rows.Close()

	out := db.Relation{
		Schema:  relationName[0],
		Table:   relationName[1],
		Columns: make([]db.Column, 0),
	}

	for rows.Next() {
		var (
			columnName   string
			isPrimaryKey bool
			dataTypeOID  uint32
			isArray      bool
		)
		if err := rows.Scan(&columnName, &isPrimaryKey, &dataTypeOID, &isArray); err != nil {
			return nil, fmt.Errorf("introspection: %w", err)
		}
		out.Columns = append(out.Columns, db.Column{
			Name:       columnName,
			ValueType:  dataTypeOID,
			IsIdentity: isPrimaryKey,
			IsArray:    isArray,
		})
	}

	return &out, nil
}
