package stream

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/samjbobb/mammoth/sync/lsn"
	"github.com/samjbobb/mammoth/target"
)

func TestStream_Stream(t *testing.T) {
	type fields struct {
		replConn      *pgconn.PgConn
		committed     *lsn.AtomicLSN
		target        target.TargetInterface
		slotName      string
		batchMaxItems int
		batchTimeout  time.Duration
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stream{
				replConn:      tt.fields.replConn,
				committed:     tt.fields.committed,
				target:        tt.fields.target,
				slotName:      tt.fields.slotName,
				batchMaxItems: tt.fields.batchMaxItems,
				batchTimeout:  tt.fields.batchTimeout,
			}
			if err := s.Stream(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Stream() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
