package bg

import (
	"context"
	"errors"

	"github.com/gotd/contrib/middleware/floodwait"
)

// client abstracts telegram client.
// implements wrapper for running client in background.
type client interface {
	Run(ctx context.Context, f func(ctx context.Context) error) error
}

// StopFunc closes client and waits until Run returns.
type StopFunc func() error

type connectOptions struct {
	ctx    context.Context
	waiter *floodwait.Waiter // New field for waiter
}

// Option for Connect.
type Option interface {
	apply(o *connectOptions)
}

type fnOption func(o *connectOptions)

func (f fnOption) apply(o *connectOptions) {
	f(o)
}

// WithContext sets base context for client.
func WithContext(ctx context.Context) Option {
	return fnOption(func(o *connectOptions) {
		o.ctx = ctx
	})
}

// WithWaiter sets a waiter for managing flood wait scenarios.
func WithWaiter(waiter *floodwait.Waiter) Option {
	return fnOption(func(o *connectOptions) {
		o.waiter = waiter
	})
}

// Connect blocks until client is connected, calling Run internally in
// background.
func Connect(client client, options ...Option) (StopFunc, error) {
	opt := &connectOptions{
		ctx: context.Background(),
	}

	for _, o := range options {
		o.apply(opt)
	}

	ctx, cancel := context.WithCancel(opt.ctx)

	errC := make(chan error, 1)
	initDone := make(chan struct{})

	go func() {
		defer close(errC)

		// Use the waiter to manage retries on FLOOD_WAIT errors.
		if opt.waiter != nil {
			errC <- opt.waiter.Run(ctx, func(ctx context.Context) error {
				return client.Run(ctx, func(ctx context.Context) error {
					close(initDone)
					<-ctx.Done()
					if errors.Is(ctx.Err(), context.Canceled) {
						return nil
					}
					return ctx.Err()
				})
			})
		} else {
			errC <- client.Run(ctx, func(ctx context.Context) error {
				close(initDone)
				<-ctx.Done()
				if errors.Is(ctx.Err(), context.Canceled) {
					return nil
				}
				return ctx.Err()
			})
		}
	}()

	select {
	case <-ctx.Done(): // context canceled
		cancel()
		return func() error { return nil }, ctx.Err()
	case err := <-errC: // startup error
		cancel()
		return func() error { return nil }, err
	case <-initDone: // init done
	}

	stopFn := func() error {
		cancel()
		return <-errC
	}
	return stopFn, nil
}
