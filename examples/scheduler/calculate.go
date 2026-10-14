package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

const (
	calculationRounds         = 100000
	cancellationCheckInterval = 1024
)

// calculate has deterministic output and one cancelable 2–5 second execution
// window. It has no external effects; another executor may safely repeat it.
func calculate(ctx context.Context, seed string, duration time.Duration) (string, error) {
	started := time.Now()
	input, err := hex.DecodeString(seed)
	if err != nil {
		return "", err
	}
	value := sha256.Sum256(input)
	for i := range calculationRounds {
		if i%cancellationCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		value = sha256.Sum256(value[:])
	}
	if delay := duration - time.Since(started); delay > 0 {
		if err := waitContext(ctx, delay); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(value[:]), ctx.Err()
}
func waitContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
