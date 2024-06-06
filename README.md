# Iceberg ACID verification

## Purpose of the project
The goal of this project is to test whether Apache Iceberg meets the ACID characteristics when using Apache Iceberg in combination with Apache Spark with multiple concurrent readers and writers.

## Test setup
To test the ACID characteristics of the Apache Iceberg setup (with Apache Spark), I've setup concurrent writers to perform modification on a table and setup concurrent readers to verify the contents of the table,
To perform this verification, the readers must know what is expected to be part of the table.
But as it's verifying the table contents during write operation, as otherwise we're not verifying isolation, what is expected to be part of the table is a bit harder to predict, as it isn't known at what moment the read will be executed.
Have some of the concurrent write operations just been committed before the actual reading starts?

One way to do this, is by setting up a kind of (in memory) "transaction log", maintained by all writers;
Before a writer performs an operation, it should log it is going to perform that operation.
After the operation is done, it should be logged that the operation is committed.
A reader can use this "transaction log" to infer which data should be available.
For example, before the reader starts reading the table, the transaction log states that:
- Transaction 1: 1 row is going to be inserted, with certain values
- Transaction 1: is committed
- Transaction 2: 1 row is going to be inserted, with certain values

What is now expected as a result of the table?
Just before the actual read operation starts, transaction 2 might be committed.
Or even a 3rd transaction might have been initiated and committed just before the actual read operation starts.
To identify what might have changed in between reading the transaction log and start performing the actual read on the table by Iceberg, we need to read the transaction log once more.
The part of the transaction log that was added after the first read of the transaction log, is the "uncertain part".
If the following is appended to the transaction log "Transaction 3: is committed", it isn't clear if the rows inserted by transaction 2 and 3 should be part of the query results.
So, the reader can verify the results in the following way:
- The row of transaction 1 must be present in the result set
- The row of transaction 2 may be present in the result set
- The row of transaction 3 may be present in the result set
- No other rows should be present

Note: When creating such a custom transaction log, it's not certain if the order of the transactions in the log is identical to the order of the actual transactions performed by Iceberg.
This is because the transaction log is not part of the transaction (and another writer might be just a bit quicker in writing the commit to the transaction log).

Deducting what is expected is probably the easiest, in case there are only "insert operations", so only transactions that add rows.
In practive it should also be able to delete and update rows.

### Verifying atomicity
To verify atomicity, more than 1 row should be included in each transaction.
That way it's possible to verify if either all rows of the transaction are visible or none

E.g>, there is a transaction inserting 2 rows.
If it is sure that the transaction must be included in the result set, both rows must be present.
If it is not sure that the transaction is part of the result set, either both rows must be present or both rows must be absent.

### Verifying consistency
To verify consistency, the parts of the result set where it is centrain that it is present in the test must match.
The transaction that we're not certain about in the test of the current reader, will be picked up by the test running after this test.
This is because these transactions we're uncertain about, are inside a "moving window" on the transaction log.

### Verifying isolation
To verify isolation, it's to verify if the rows affected by a transaction are visible just before the commit.
But, given the way Apache Iceberg works, that is very unlikely, as these modifications are part of a new snapshot and we only become aware of this snapshot once that snapshot is added to the new version of the table in the metadata.

What we can verify is that a transaction is not partially visible.
This is effectively the same check as one to verify atomicity.

### Verifying durability
To verify durability, we should verify that the contents we're sure about, are part of the result set.

## Running the verification
To run the verification, you'll first need to start Docker, as the test relies on test containers.
After Docker has been started, you can run the test in `TransactionManagerTest`.
Note that this test doesn't run on a distributed file system, like HDFS, unless you have it installed on the machine you're running it on.

# Test results
The test fails with the message "ACID Verification failed", which is pretty clear.
It's also visible in the console log, where it says "ReaderThread: Verification Failed".
What is exacally is happening and why is unclear.