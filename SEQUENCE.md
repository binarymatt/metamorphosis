Sequence

# Manager

Loop:

1. DescribeStream
   - if stream status is DELETING OR CREATING
   - log error and cancel context
   - get shard list
2. GetCurrentReservations for group
3. get current actor details
4. if actor count under limit
5. for each available shard (shard is not assignd or closed)
6. create actor, assign shard

# Actor

- Reserve Shard
- WaitOnParent
- Fetch Records
  - process record
  - if error, exit
  - commit record
- Check Next Iterator
- if nil, close shard
-
