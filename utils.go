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

// NewSafeGroup creates a SafeGroup backed by errgroup.WithContext.
//
// The returned SafeGroup:
// - shares a derived context across goroutines (canceled on parent cancellation or first non-nil error),
// - can restart goroutines on panic via GoSafe,
// - can wait with interruption semantics via WaitOrInterrupt.
func NewSafeGroup(ctx context.Context) *SafeGroup {
	if ctx == nil {
		ctx = context.Background()
	}
	parent := ctx
	group, groupCtx := errgroup.WithContext(ctx)
	return &SafeGroup{Group: group, ctx: groupCtx, parent: parent}
}

// SafeGroup is an errgroup.Group with safer defaults for long-running workers.
//
// It provides:
// - GoSafe: runs a worker with panic recovery + restart backoff.
// - WaitOrInterrupt: waits for group completion, returning early on external interruption.
type SafeGroup struct {
	*errgroup.Group
	// ctx is the errgroup-derived context (canceled on parent cancellation or first non-nil error).
	ctx context.Context
	// parent is the caller-provided context (typically signal.NotifyContext).
	// WaitOrInterrupt uses this (instead of sg.ctx) so "errgroup canceled because a worker returned an error"
	// is preserved as a real error rather than being normalized into context.Canceled.
	parent context.Context
}

// GoSafe runs fn in an errgroup goroutine, logs panics to stderr, and restarts
// the goroutine with exponential backoff.
//
// Notes:
//   - Panics are treated as recoverable: they will not cancel sibling goroutines.
//   - Returned errors preserve errgroup semantics: returning a non-nil error will cancel the
//     group's derived context and make Wait() return that error.
//   - Context cancellation stops the restart loop so Wait() can return promptly.
//
// We intentionally avoid structured logging here: panics may be caused by the
// logger itself, so printing to stderr is the safest fallback.
func (sg *SafeGroup) GoSafe(name string, fn func(context.Context) error) {
	if sg == nil || sg.Group == nil || fn == nil {
		return
	}
	sg.Group.Go(func() (err error) {
		// Restart the goroutine on panic, log to stderr, and keep other goroutines running.
		// Exit promptly when ctx is canceled so errgroup.Wait() can return.
		backoff := 200 * time.Millisecond
		const maxBackoff = 30 * time.Second
		for {
			if sg.ctx != nil {
				select {
				case <-sg.ctx.Done():
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
				err = fn(sg.ctx)
			}()

			if panicked {
				// Avoid structured logging here: the panic might be caused by the logger itself.
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

			// If the worker returns an error, preserve errgroup semantics (cancel siblings).
			return err
		}
	})
}

// WaitOrInterrupt waits for the group's goroutines to finish, but returns early
// with sg.parent.Err() if the parent context is canceled.
//
// Behavior:
// - If the parent context is nil, it simply waits for group completion.
// - If the parent context is done before Wait returns:
//   - If gracePeriod <= 0, returns parent.Err() immediately.
//   - Otherwise waits up to gracePeriod for Wait to finish; if it doesn't, returns parent.Err().
//
// - If Wait returns an error that is (or matches) parent.Err(), it is normalized to parent.Err().
func (sg *SafeGroup) WaitOrInterrupt(gracePeriod time.Duration) error {
	if sg == nil || sg.Group == nil {
		return nil
	}
	ctx := sg.parent
	wait := sg.Group.Wait
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

// normalizeInterruptError maps context cancellation errors to ctx.Err().
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
