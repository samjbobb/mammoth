package lsn

import (
	"fmt"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

type AtomicLSN struct {
	sync.RWMutex
	lsn pglogrepl.LSN
}

func NewAtomicLSN() *AtomicLSN {
	return &AtomicLSN{}
}

func (s *AtomicLSN) Read() pglogrepl.LSN {
	s.RLock()
	defer s.RUnlock()
	return s.lsn
}

func (s *AtomicLSN) Set(lsn pglogrepl.LSN) {
	s.Lock()
	defer s.Unlock()
	s.lsn = lsn
}

func (s *AtomicLSN) Update(newLsn pglogrepl.LSN) error {
	s.Lock()
	defer s.Unlock()
	curLsn := s.lsn
	if newLsn > curLsn {
		s.lsn = newLsn
		logrus.WithField("lsn", newLsn.String()).Debugln("update lsn")
		// "The location of the last WAL byte + 1 received and written to disk in the standby."
		// - https://www.postgresql.org/docs/10/protocol-replication.html
	} else if newLsn < curLsn {
		return fmt.Errorf("unexpected lsn, newLsn: %v, curLsn: %v", newLsn, curLsn)
	}
	return nil
}
