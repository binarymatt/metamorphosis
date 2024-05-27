resource "aws_dynamodb_table" "checkpoint_table" {
  name         = "metamorphosis_reservations"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "groupID"
  range_key    = "shardID"

  attribute {
    name = "groupID"
    type = "S"
  }

  attribute {
    name = "shardID"
    type = "S"
  }
}
