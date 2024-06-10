package metamorphosis

import (
	"context"
	"log/slog"
)

type ClientContextKey struct{}
type LoggerContextKey struct{}

func LoggerWithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, LoggerContextKey{}, logger)
}
func LoggerFromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(LoggerContextKey{}).(*slog.Logger)
	if !ok {
		return slog.Default()
	}
	return logger
}
func ClientWithContext(ctx context.Context, client *Client) context.Context {
	return context.WithValue(ctx, ClientContextKey{}, client)
}
func ClientFromContext(ctx context.Context) *Client {
	api, ok := ctx.Value(ClientContextKey{}).(*Client)
	if !ok {
		return nil
	}
	return api
}
