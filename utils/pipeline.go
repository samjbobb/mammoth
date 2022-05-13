package utils

import (
	"context"
	"sync"
)

func WaitForPipeline(ctx context.Context, errs ...<-chan error) error {
	errChan := MergeErrors(ctx, errs...)
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func MergeErrors(ctx context.Context, errChans ...<-chan error) <-chan error {
	var wg sync.WaitGroup

	out := make(chan error, len(errChans))

	wg.Add(len(errChans))
	output := func(c <-chan error) {
		defer wg.Done()
		for err := range c {
			select {
			case <-ctx.Done():
				return
			case out <- err:
			}
		}
	}
	for _, errChan := range errChans {
		go output(errChan)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
