package metamorphosis

import (
	"context"
	"log/slog"
)

type clientContextKey struct{}
type loggerContextKey struct{}

// LOgerWithContext adds the given logger to the returned Context
func LoggerWithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey{}, logger)
}

// LoggerFromContext retrieves a logger from the passed in Context or returns the default slog.Logger
func LoggerFromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(loggerContextKey{}).(*slog.Logger)
	if !ok {
		return slog.Default()
	}
	return logger
}

// ClientWithContext adds the given client to the returned Context
func ClientWithContext(ctx context.Context, client *Client) context.Context {
	return context.WithValue(ctx, clientContextKey{}, client)
}

// ClientFromContext retrieves a Client from Context, if it exists in the given context.
func ClientFromContext(ctx context.Context) *Client {
	api, ok := ctx.Value(clientContextKey{}).(*Client)
	if !ok {
		return nil
	}
	return api
}
