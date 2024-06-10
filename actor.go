package metamorphosis

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	metamorphosisv1 "github.com/binarymatt/metamorphosis/gen/metamorphosis/v1"
)

var ErrWaitTimePassed = errors.New("wait time passed")

type RecordProcessor = func(context.Context, *metamorphosisv1.Record) error
type Actor struct {
	id                   string
	mc                   *Client
	processor            RecordProcessor
	logger               *slog.Logger
	SleepAfterProcessing time.Duration
	batchSize            int32
	shard                types.Shard
}

func (a *Actor) WaitForParent(ctx context.Context) error {
	// check reservation table to see if it's closed
	if a.shard.ParentShardId == nil {
		return nil
	}
	if len(*a.shard.ParentShardId) == 0 {
		return nil
	}
	counter := 0
	for {
		if counter == 150 {
			return ErrWaitTimePassed
		}
		a.logger.Debug("waiting on parent to finish", "parent_shard", *a.shard.ParentShardId)
		res, err := a.mc.fetchReservation(ctx, *a.shard.ParentShardId)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			return err
		}
		if res.LatestSequence == ShardClosed {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (a *Actor) Work(ctx context.Context) error {
	reservation := a.mc.CurrentReservation()
	if reservation == nil {
		return ErrMissingReservation
	}
	//TODO wait on parent
	a.logger = a.logger.With("shard_id", reservation.ShardID, "worker_id", reservation.WorkerID, "group_id", reservation.GroupID)
	err := a.WaitForParent(ctx)
	if err != nil {
		if errors.Is(err, ErrWaitTimePassed) {
			return nil
		}
		a.logger.Error("error waiting on parent shard to finish", "error", err)
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// see if shard is still active
			a.logger.Debug("checking if shard closed")
			if closed, err := a.mc.IsIteratorClosed(ctx); err != nil || closed {
				a.logger.Debug("shard is closed or error checking")
				if err != nil {
					a.logger.Error("error checking shard", "error", err)
					continue
				}
				a.logger.Warn("closing shard")
				if err := a.mc.CloseShard(ctx); err != nil {
					a.logger.Error("could not close shard", "error", err)
					return err
				}
				a.logger.Warn("shard is closed")
				// Exit out to let other actors spin up if new shars are ready
				return nil

			}

			a.logger.Debug("fetching record")
			records, err := a.mc.FetchRecords(ctx, a.batchSize)
			if err != nil {
				a.logger.Error("error fetching record(s)", "error", err, "batch_size", a.batchSize)
				return err
			}
			if records == nil || len(records) < 1 {
				a.logger.Warn("records is empty. sleeping for 5 seconds")
				time.Sleep(5 * time.Second)
				continue
			}
			for _, record := range records {
				if record == nil {
					a.logger.Debug("record is nil")
					continue
				}
				a.logger.Debug("processing record", "record", record)

				cx := LoggerWithContext(ctx, a.logger)
				cx = ClientWithContext(cx, a.mc)
				// ctxWithLogger := context.WithValue(ctxWithClient, LoggerContextKey{}, a.logger)
				if err := a.processor(cx, record); err != nil {
					a.mc.ClearIterator()
					a.logger.Error("error processing record", "error", err, "processor", a.processor)
					return err
				}
				a.logger.Debug("commiting record")
				if err := a.mc.CommitRecord(ctx, record); err != nil {
					a.logger.Error("error commiting record", "error", err)
					a.mc.ClearIterator()
					return err
				}
			}
			a.logger.Debug("checking sleep", "sleep_time", a.SleepAfterProcessing)
			if a.SleepAfterProcessing != 0 {
				a.logger.Warn("sleeping after processing", "duration", a.SleepAfterProcessing)
				time.Sleep(a.SleepAfterProcessing)
			}
		}
	}
}
