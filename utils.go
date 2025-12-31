package taskagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"golang.org/x/sync/errgroup"
)

// GroupGoSafe runs fn in an errgroup goroutine, logs panics to stderr, and
// restarts the goroutine with exponential backoff.
//
// Notes:
//   - Panics are treated as recoverable: they will not cancel sibling goroutines.
//   - Returned errors preserve errgroup semantics: returning a non-nil error will
//     cancel the group's derived context (when using errgroup.WithContext) and
//     make Wait() return that error.
//   - ctx cancellation stops the restart loop so Wait() can return promptly.
//
// We intentionally avoid structured logging here: panics may be caused by the
// logger itself, so printing to stderr is the safest fallback.
func GroupGoSafe(ctx context.Context, group *errgroup.Group, name string, fn func(context.Context) error) {
	if group == nil || fn == nil {
		return
	}
	group.Go(func() (err error) {
		backoff := 200 * time.Millisecond
		const maxBackoff = 30 * time.Second
		for {
			if ctx != nil {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
			}

			panicked := false
			var recovered any
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
						recovered = r
					}
				}()
				err = fn(ctx)
			}()

			if panicked {
				_, _ = fmt.Fprintf(os.Stderr, "WARN: %s panicked: %v\n%s\n", name, recovered, debug.Stack())

				// Add a small deterministic jitter without relying on math/rand.
				jitterMax := backoff / 2
				jitter := time.Duration(0)
				if jitterMax > 0 {
					jitter = time.Duration(time.Now().UnixNano() % int64(jitterMax))
				}
				time.Sleep(backoff + jitter)

				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			return err
		}
	})
}

// WaitOrInterrupt waits for wait() to return, but returns ctx.Err() if ctx is done.
//
// Behavior:
// - If ctx is nil, it simply returns wait().
// - If ctx is done before wait() returns:
//   - If gracePeriod <= 0, returns ctx.Err() immediately.
//   - Otherwise waits up to gracePeriod for wait() to finish; if it doesn't, returns ctx.Err().
//
// - If wait() returns an error that is (or matches) ctx.Err(), it is normalized to ctx.Err().
func WaitOrInterrupt(ctx context.Context, wait func() error, gracePeriod time.Duration) error {
	if ctx == nil {
		return wait()
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- wait()
	}()

	select {
	case err := <-waitCh:
		return normalizeInterruptError(ctx, err)
	case <-ctx.Done():
		if gracePeriod <= 0 {
			return ctx.Err()
		}
		select {
		case err := <-waitCh:
			return normalizeInterruptError(ctx, err)
		case <-time.After(gracePeriod):
			return ctx.Err()
		}
	}
}

// normalizeInterruptError maps context-cancellation errors to ctx.Err().
func normalizeInterruptError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	if ctx != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return ctx.Err()
	}
	return err
}
