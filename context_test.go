package metamorphosis

import (
	"context"
	"log/slog"
	"testing"

	"github.com/shoenig/test/must"
)

func TestLoggerWithContext(t *testing.T) {
	logger := slog.Default()
	ctx := context.Background()
	ctx2 := LoggerWithContext(ctx, logger)
	must.NotEq(t, ctx2, ctx)
	value := ctx2.Value(loggerContextKey{})
	must.NotNil(t, value)
	lgr, ok := value.(*slog.Logger)
	must.True(t, ok)
	must.Eq(t, logger, lgr)
}

func TestLoggerFromContext_Default(t *testing.T) {
	ctx := context.Background()
	logger := LoggerFromContext(ctx)
	must.Eq(t, slog.Default(), logger)
}

func TestLoggerFromContext_Exists(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default().With("test", true)
	ctx = context.WithValue(ctx, loggerContextKey{}, logger)
	lgr := LoggerFromContext(ctx)
	must.NotEq(t, slog.Default(), lgr)
	must.Eq(t, logger, lgr)
}

func TestClientWithContext(t *testing.T) {
	ctx := context.Background()
	c := ctx.Value(clientContextKey{})
	must.Nil(t, c)
	client := NewClient(testConfig())
	ctx = ClientWithContext(ctx, client)
	value := ctx.Value(clientContextKey{})
	must.NotNil(t, value)
}

func TestClientFromContext(t *testing.T) {
	ctx := context.Background()
	c := ClientFromContext(ctx)
	must.Nil(t, c)
	client := NewClient(testConfig())
	ctx = context.WithValue(ctx, clientContextKey{}, client)
	c = ClientFromContext(ctx)
	must.NotNil(t, c)
	must.Eq(t, client, c)
}
