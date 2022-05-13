package csvsplit

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
)

type RecordTransformer interface {
	Transform([]string) ([]string, error)
}

type PassThrough struct{}

func (t *PassThrough) Transform(in []string) ([]string, error) {
	return in, nil
}

// Split splits a CSV file into multiple CSV files of no more than rowsPerChunk in each output file.
// When useHeader is true, the header row from the input file is included in each output file.
func Split(ctx context.Context, rowsPerChunk int, useHeader bool, in io.Reader, recXformer RecordTransformer) (<-chan io.Reader, <-chan error, error) {
	if rowsPerChunk < 1 {
		return nil, nil, fmt.Errorf("rowsPerChunk must be a positive number")
	}
	out := make(chan io.Reader)
	errChan := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errChan)
		// It might seem odd to read (parse) the CSV and then immediately write (serialize) it again. However,
		// splitting a CSV file on new lines not always correct. New lines contained in records are not escaped in
		// CSV files. So to determine the valid row boundary to split on, a nearly full CSV parser is needed.
		r := csv.NewReader(in)
		var header []string

		var buf *bytes.Buffer
		var w *csv.Writer
		recordsInChunk := 0
		for {
			record, err := r.Read()
			if err == io.EOF {
				if buf != nil && w != nil && recordsInChunk > 0 {
					// finalize and send the last chunk
					w.Flush()
					//	send the chunk to the output channel, returning early if the context is cancelled
					select {
					case out <- buf:
					case <-ctx.Done():
						return
					}
				}
				return
			} else if err != nil {
				errChan <- err
				return
			}
			if useHeader && header == nil {
				// save the header row for use in each chunk
				header = record
				continue
			}
			if buf == nil {
				// initialize and write header for this chunk
				buf = new(bytes.Buffer)
				w = csv.NewWriter(buf)
				if useHeader {
					if err := w.Write(header); err != nil {
						errChan <- err
						return
					}
				}
			}
			// transform the record
			xformedRec, err := recXformer.Transform(record)
			if err != nil {
				errChan <- err
				return
			}

			// write record to chunk and increment the record counter
			if err := w.Write(xformedRec); err != nil {
				errChan <- err
				return
			}
			recordsInChunk++

			// chunk is full, so finalize, send, and reset
			if recordsInChunk >= rowsPerChunk {
				w.Flush()
				// send the chunk to the output channel, returning early if the context is cancelled
				select {
				case out <- buf:
				case <-ctx.Done():
					return
				}
				//reset for next chunk
				buf = nil
				w = nil
				recordsInChunk = 0
			}
		}
	}()
	return out, errChan, nil
}
