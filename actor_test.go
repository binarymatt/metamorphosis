package metamorphosis

import (
	"context"
	"testing"

	"github.com/shoenig/test/must"
)

func TestActorWork_NoReservation(t *testing.T) {
	config := testConfig()
	cl := NewClient(config)
	a := Actor{
		id: "test",
		mc: cl,
	}
	err := a.Work(context.Background())
	must.ErrorIs(t, err, ErrMissingReservation)
}
