package service

import (
	"fmt"
	"testing"

	"github.com/jackc/pgtype"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPgType(t *testing.T) {
	ci := pgtype.NewConnInfo()
	var out []string
	ci.Scan(pgtype.TextArrayOID, pgtype.TextFormatCode, []byte("{foo,bar,bim}"), &out)
	fmt.Println(out)
}
