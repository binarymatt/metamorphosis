# Metamorphosis
Inspired by [vmware-go-kcl-v2](https://github.com/vmware/vmware-go-kcl-v2) and [kafka-go](https://github.com/segmentio/kafka-go) 
this is an opinionated library for interacting with kinesis from Go.

## Config
### Required Fields
**GroupID**
The group the client is a member of.
The group id is used as a partition key in Dynamo for related workers.

**WorkerID**
WorkerID is the identifier used for shard reservations.

**StreamARN**
StreamARN is the kinesis data stream arn used for operations.

**KinesisClient**
Kinesis client is used to connect to the AWS kinesis apis.

**DynamoClient**
Dynamodb client is used to connect to Dynamo and save stream/shard positions.

### Optional Fields
**ReservationTableName**
ReservationTableName is the name of the dynamo table that reservation state is stored in.

**ShardID**
ShardID will allow a client to explicitly mark the shard it will read from.

**ReservationTimeout**
ReservationTimeout indicates when a reservation will be considered expired.

**RenewTime**
If set, RenewTime tells the client to start a goroutine to automatically renew reservations.

**ManagerLoopWaitTime**
When using the default client, the ManagerLoopWaitTime indicates how long
between control loop runs to wait.

**RecordProcessor** 
When using the default client, the RecordProcessor allows users
to process a record and return an error if there is an issue.

**Logger**
Logger allows users to pass in a custom slog.Logger

**ShardCacheDuration**
ShardCacheDuration determines how often to check for new shards on a dynamic kinesis stream.

**MaxActorCount**
Max number of actors (goroutines) that a manger is allowed to instantiate to process shards.

**WorkerPrefix**
WorkerPrefix is used in the manager to name the actor(s) WorkerID

**SleepAfterProcessing**
if present, SleepAfterProcessing tells the manager to sleep for the duration before getting a new record.

**Seed**
Seed is used when multiple clients are trying to reserve shards, Seed is the position to start trying to reserve from.


## Basic Client
```go
import (
  "github.com/binarymatt/metamorphosis"
)

func main(){
  client := metamorphosis.NewClient()
}
```
## Default Client
Also called the manager, this client handles retrieval and committing of records.
```
cfg := metamorphosis.NewConfig()

metamorphosis.New(context.Background(), cfg)
```
