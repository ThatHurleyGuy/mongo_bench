# mongo_bench

A tool to simulate load on a mongo cluster. Can be used in tandem with the mongobetween dual read feature to stress test that functionality to see latency impact.

## Workers

Meant to simulate different types of workload with command line flags to change the number of workers. Each worker is a goroutine that is in a loop that continues to repeat the type of operation, while every once in a while reporting the number of operations and time spent on those operations back to a main thread that aggregates the results and prints them out to the user.

### Insert
Insert a single random "transaction"

### Id Read
Reads a single random "transaction" using the _id field

### Update
Updates a single random "transaction" using the _id field, just updates the amount field to some other random amount

### Aggregation
Meant to simulate a sort of report. Groups the transactions by `category`, and sums up the total amounts for the last 5 seconds. Should be a much slower operation than the others.
