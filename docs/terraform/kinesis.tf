resource "aws_kinesis_stream" "event_stream" {
  name = "metamorphosis_stream"

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
    "IncomingRecords",
    "OutgoingRecords",
    "IteratorAgeMilliseconds",
    "ReadProvisionedThroughputExceeded",
    "WriteProvisionedThroughputExceeded"
  ]

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}
