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

func NewSafeGroup(ctx context.Context) *SafeGroup {
	if ctx == nil {
		ctx = context.Background()
	}
	parent := ctx
	group, groupCtx := errgroup.WithContext(ctx)
	return &SafeGroup{Group: group, ctx: groupCtx, parent: parent}
}

type SafeGroup struct {
	*errgroup.Group
	// ctx is the errgroup-derived context (canceled on parent cancellation or first error).
	ctx context.Context
	// parent is the caller-provided context (typically signal.NotifyContext).
	// WaitOrInterrupt uses this to avoid treating "errgroup canceled because of worker error"
	// as an external interrupt.
	parent context.Context
}

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
