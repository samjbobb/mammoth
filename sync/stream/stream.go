package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/samjbobb/mammoth/sync/db"
	"github.com/samjbobb/mammoth/sync/lsn"
	"github.com/samjbobb/mammoth/target"
	"github.com/samjbobb/mammoth/utils"
	"github.com/sirupsen/logrus"
)

const (
	pgOutputPlugin = "pgoutput"
)

type Stream struct {
	pool            *pgxpool.Pool
	replConn        *pgconn.PgConn
	committed       *lsn.AtomicLSN
	target          target.TargetInterface
	slotName        string
	batchMaxItems   int
	batchTimeout    time.Duration
	skipAcknowledge bool
}

func NewStream(pool *pgxpool.Pool, replConn *pgconn.PgConn, target target.TargetInterface, slotName string, batchMaxItems int, batchTimeout time.Duration, skipAcknowledge bool) *Stream {
	return &Stream{
		pool:            pool,
		replConn:        replConn,
		committed:       lsn.NewAtomicLSN(),
		target:          target,
		slotName:        slotName,
		batchMaxItems:   batchMaxItems,
		batchTimeout:    batchTimeout,
		skipAcknowledge: skipAcknowledge,
	}
}

func (s *Stream) SetRestartLSN(restartLSN pglogrepl.LSN) {
	s.committed.Set(restartLSN)
}

func (s *Stream) CreateSlot(ctx context.Context) (string, error) {
	// Create the replication slot, returning the consistent starting point (LSN) and the snapshot name which allows
	// querying the tables at exactly the state before the replication slot beings producing events.
	slotResult, err := pglogrepl.CreateReplicationSlot(ctx, s.replConn, s.slotName, pgOutputPlugin, pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		return "", fmt.Errorf("createReplicationSlot failed: %w", err)
	}
	logrus.Println("created replication slot:", slotResult.SlotName, "snapshot name:", slotResult.SnapshotName, "sonsistent point:", slotResult.ConsistentPoint)
	consistentPointLsn, err := pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		return "", fmt.Errorf("parseLSN failed:: %w", err)
	}
	logrus.Println("consistent point lsn:", consistentPointLsn)
	s.committed.Set(consistentPointLsn)
	return slotResult.SnapshotName, nil
}

func (s *Stream) updateCommitted(txs ...*db.WalTransaction) error {
	logrus.WithFields(logrus.Fields{
		"numTx": len(txs),
	}).Debugln("updateCommitted")
	var newLsn pglogrepl.LSN
	for _, tx := range txs {
		if tx.LSN > newLsn {
			newLsn = tx.LSN
		}
	}
	// "The location of the last WAL byte + 1 received and written to disk in the standby."
	// - https://www.postgresql.org/docs/10/protocol-replication.html
	return s.committed.Update(newLsn + 1)
}

// receiveMessageWithDeadline receives one wire protocol message from the PostgreSQL server. It blocks until a message
// is available or deadline expires.
func (s *Stream) receiveMessageWithDeadline(ctx context.Context, deadline time.Time) (pgproto3.BackendMessage, error) {
	receiveMessageCtx, receiveMessageCancel := context.WithDeadline(ctx, deadline)
	defer receiveMessageCancel()
	return s.replConn.ReceiveMessage(receiveMessageCtx)
}

// Close is a finalizer function.
func (s *Stream) Close(ctx context.Context) error {
	var err error
	err = s.replConn.Close(ctx)
	if err != nil {
		return err
	}
	return nil
}

// eventSource starts a goroutine to receive messages from Postgres, accumulate multiple messages into WAL transactions,
// sends the transactions on the returned channel, and periodically sends status updates to Postgres.
// The status messages to Postgres keep the connection alive and confirm receipt of messages by sending the current LSN.
// Returns immediately after starting a new goroutine. The goroutine can be ended by cancelling the Context.
func (s *Stream) eventSource(ctx context.Context) (<-chan *db.WalTransaction, <-chan error, error) {
	out := make(chan *db.WalTransaction)
	errChan := make(chan error, 1)
	standbyMessageTimeout := time.Second * 10

	sendEvent := func(logicalTx *db.WalTransaction) {
		for {
			select {
			case <-ctx.Done():
				return
			case out <- logicalTx:
				return
			case <-time.After(standbyMessageTimeout):
				pubLsn := s.committed.Read()
				err := pglogrepl.SendStandbyStatusUpdate(ctx, s.replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pubLsn})
				if err != nil {
					errChan <- fmt.Errorf("sendStandbyStatusUpdate failed: %w", err)
					return
				}
				logrus.WithField("lsn", pubLsn.String()).Debugln("sent standby status message while processing pipeline is blocked")
			}
		}
	}

	typeIsArray, err := s.introspectArrayTypes(ctx)
	if err != nil {
		return nil, nil, err
	}

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", s.slotName)}
	err = pglogrepl.StartReplication(ctx, s.replConn, s.slotName, s.committed.Read(), pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return nil, nil, fmt.Errorf("startReplication failed: %w", err)
	}
	logrus.Infoln("logical replication started")

	go func() {
		defer close(out)
		defer close(errChan)
		nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
		wta := NewAccumulator(typeIsArray)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if time.Now().After(nextStandbyMessageDeadline) {
				pubLsn := s.committed.Read()
				err := pglogrepl.SendStandbyStatusUpdate(ctx, s.replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pubLsn})
				if err != nil {
					errChan <- fmt.Errorf("sendStandbyStatusUpdate failed: %w", err)
					return
				}
				logrus.WithField("lsn", pubLsn.String()).Infoln("sent standby status message to postgres")
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}

			// Blocks until a message is available or deadline expires
			msg, err := s.receiveMessageWithDeadline(ctx, nextStandbyMessageDeadline)
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				errChan <- fmt.Errorf("receiveMessage failed: %w", err)
				return
			}
			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						errChan <- fmt.Errorf("parsePrimaryKeepaliveMessage failed: %w", err)
						return
					}
					logrus.WithFields(logrus.Fields{
						"ServerWalEnd":   pkm.ServerWALEnd.String(),
						"ServerTime":     pkm.ServerTime,
						"ReplyRequested": pkm.ReplyRequested}).
						Debugln("received primary keepalive message")

					if pkm.ReplyRequested {
						nextStandbyMessageDeadline = time.Time{}
					}
					// Send an empty transaction with the server wal, so the client can continue to
					// acknowledge the advancing wal even if there are no publishable events.
					// This avoids the replication slot falling behind when there are many changes on tables
					// that are not in Config.Sync.Tables
					sendEvent(&db.WalTransaction{
						LSN:        pkm.ServerWALEnd,
						BeginTime:  nil,
						CommitTime: nil,
						Actions:    nil,
					})

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						errChan <- fmt.Errorf("parseXLogData failed: %w", err)
						return
					}
					logrus.WithFields(logrus.Fields{
						"WALStart":     xld.WALStart.String(),
						"ServerWALEnd": xld.ServerWALEnd,
						"ServerTime":   xld.ServerTime,
					}).Debugln("received XLogData")
					logicalMsg, err := pglogrepl.Parse(xld.WALData)
					if err != nil {
						errChan <- fmt.Errorf("parse logical replication message: %w", err)
						return
					}

					logicalTx, err := wta.Add(logicalMsg)
					if err == ErrRelationChanged {
						logrus.Warnln("Relation changed")
						errChan <- err
						return
					} else if err != nil {
						logrus.WithError(err).Errorln("msg parse failed")
						errChan <- fmt.Errorf("%v: %w", "unmarshal wal message error", err)
						return
					} else if logicalTx != nil {
						logrus.WithFields(logrus.Fields{
							"lsn":           logicalTx.LSN.String(),
							"commitTime":    logicalTx.CommitTime,
							"numRowChanges": len(logicalTx.Actions),
						}).Debugln("logical transaction assembled from wal")

						sendEvent(logicalTx)
						logrus.WithFields(logrus.Fields{
							"lsn":           logicalTx.LSN.String(),
							"commitTime":    logicalTx.CommitTime,
							"numRowChanges": len(logicalTx.Actions),
						}).Debugln("logical transaction sent to pipeline")
					}
				}
			default:
				logrus.Warnf("received unexpected message: %#v\n", msg)
			}
		}
	}()
	return out, errChan, nil
}

// eventBatcher starts a goroutine that accepts WAL Transactions on the passed channel, batches them, and sends them on the returned channel.
// Batches are delineated by time and size: a batch is sent when it exceeds batchMaxItems in size or batchTimeout has passed since the last batch was sent.
// Returns immediately after starting a new goroutine. The goroutine can be ended by cancelling the Context.
func (s *Stream) eventBatcher(ctx context.Context, in <-chan *db.WalTransaction) (<-chan []*db.WalTransaction, <-chan error, error) {
	// Making this a buffered channel allows some reading from Postgres while writing to Snowflake
	// The buffer needs to be small enough that it fits entirely in memory
	out := make(chan []*db.WalTransaction, 5)
	errChan := make(chan error, 1)

	actionsInBatch := func(batch []*db.WalTransaction) int {
		num := 0
		for _, el := range batch {
			num += len(el.Actions)
		}
		return num
	}

	go func() {
		defer close(out)
		defer close(errChan)
		for inputChanOpen := true; inputChanOpen; {
			var batch []*db.WalTransaction
			expire := time.After(s.batchTimeout)
			for {
				select {
				case <-ctx.Done():
					return
				case value, ok := <-in:
					if !ok {
						inputChanOpen = false
						logrus.WithFields(logrus.Fields{
							"reason":          "inputChanClosed",
							"numTransactions": len(batch),
						}).Debugln("batcher send batch")
						goto send
					}

					batch = append(batch, value)

					numActions := actionsInBatch(batch)

					if numActions >= s.batchMaxItems {
						logrus.WithFields(logrus.Fields{
							"reason":          "batchMaxItems",
							"numTransactions": len(batch),
							"actionsInBatch":  numActions,
						}).Debugln("batcher send batch")
						goto send
					}

				case <-expire:
					logrus.WithFields(logrus.Fields{
						"reason":          "expire",
						"numTransactions": len(batch),
						"actionsInBatch":  actionsInBatch(batch),
					}).Debugln("batcher send batch")
					goto send
				}
			}

		send:
			if len(batch) > 0 {
				out <- batch
			}
		}
	}()
	return out, errChan, nil
}

// eventSink starts a new goroutine that receives batches of WAL Transactions and writes them to the target data store.
// Returns immediately after starting a new goroutine. The goroutine can be ended by cancelling the Context.
func (s *Stream) eventSink(ctx context.Context, in <-chan []*db.WalTransaction) (<-chan error, error) {
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		for wtxBatch := range in {
			var filteredBatch []*db.WalTransaction
			for _, wtx := range wtxBatch {
				if len(wtx.Actions) > 0 {
					filteredBatch = append(filteredBatch, wtx)
				}
			}
			if err := s.target.Write(ctx, filteredBatch); err != nil {
				errChan <- fmt.Errorf("could not write batch to target: %w", err)
				return
			} else if !s.skipAcknowledge {
				// Update LSN to acknowledge messages
				if err := s.updateCommitted(wtxBatch...); err != nil {
					errChan <- err
					return
				}
			} else {
				logrus.Infoln("skipping acknowledge")
			}
		}
	}()
	return errChan, nil
}

// Stream receives event from PostgreSQL, batches them, calls target.Write for each batch, increments the LSN
// after each successful call to Write, sends StandbyStatus after each successful Write call with the updated LSN,
// and sends periodic StandbyStatus as heartbeats.
func (s *Stream) Stream(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var errChans []<-chan error

	wtxChan, errChan, err := s.eventSource(ctx)
	if err != nil {
		return err
	}
	errChans = append(errChans, errChan)

	wtxBatchChan, errChan, err := s.eventBatcher(ctx, wtxChan)
	if err != nil {
		return err
	}
	errChans = append(errChans, errChan)

	errChan, err = s.eventSink(ctx, wtxBatchChan)
	if err != nil {
		return err
	}
	errChans = append(errChans, errChan)

	logrus.Infoln("streaming started")

	return utils.WaitForPipeline(ctx, errChans...)
}

func (s *Stream) jsonLinesSink(_ context.Context, in <-chan *db.WalTransaction, outFile string) (<-chan error, error) {
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)

		f, err := os.Create(outFile)
		if err != nil {
			errChan <- err
			return
		}
		defer func() {
			utils.PanicIfErr(f.Close())
		}()
		e := json.NewEncoder(f)
		for wtx := range in {
			if err := e.Encode(wtx); err != nil {
				errChan <- err
				return
			}
			if err := f.Sync(); err != nil {
				errChan <- err
				return
			}
		}

	}()
	return errChan, nil
}

func (s *Stream) StreamToJSONLines(ctx context.Context, outFile string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var errChans []<-chan error

	wtxChan, errChan, err := s.eventSource(ctx)
	if err != nil {
		return err
	}
	errChans = append(errChans, errChan)

	errChan, err = s.jsonLinesSink(ctx, wtxChan, outFile)
	if err != nil {
		return err
	}
	errChans = append(errChans, errChan)

	logrus.Infoln("streaming started")

	return utils.WaitForPipeline(ctx, errChans...)
}

func (s *Stream) introspectArrayTypes(ctx context.Context) (map[uint32]bool, error) {
	const query = `
	select pgt.oid                                   as data_type_oid,
		   format_type(pgt.oid, null::integer)         as data_type_name,
		   pgt.typcategory = 'A'                       as is_array,
		   coalesce(subpgt.typtype, pgt.typtype) = 'e' as is_enum
	from pg_type as pgt
			 left outer join pg_type as subpgt
							 on pgt.typelem = subpgt.oid
								 and pgt.typelem != 0
	order by data_type_oid;`
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("introspect types: %w", err)
	}
	defer rows.Close()

	out := make(map[uint32]bool)

	for rows.Next() {
		var (
			dataTypeOID  uint32
			dataTypeName string
			isArray      bool
			isEnum       bool
		)
		if err := rows.Scan(&dataTypeOID, &dataTypeName, &isArray, &isEnum); err != nil {
			return nil, fmt.Errorf("introspect types: %w", err)
		}
		out[dataTypeOID] = isArray
	}
	return out, nil
}
