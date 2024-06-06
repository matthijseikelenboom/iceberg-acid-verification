package org.example.writer;

import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.example.reader.ReaderThread;
import org.example.resultset.Record;
import org.example.transactionlog.Transaction;
import org.example.transactionlog.TransactionLog;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionManager {
    private final Configuration configuration;
    private final SparkSession session;
    private final TransactionLog transactionLog;
    private final TransactionGenerator transactionGenerator;
    private final String fullyQualifiedTableName;
    private final AtomicInteger transactionCount;
    private final AtomicBoolean stopReadersAndWriters;
    private final AtomicInteger failedVerificationCount;

    @Getter
    private boolean hasFailedWriters;

    @Getter
    private boolean hasFailedReaders;

    public TransactionManager(Configuration configuration, SparkSession session) {
        this.configuration = configuration;
        this.session = session;
        this.transactionLog = new TransactionLog();
        this.transactionGenerator = new TransactionGenerator(configuration);
        this.fullyQualifiedTableName = String.format("%s.%s.%s", configuration.getCatalogName(), configuration.getDatabaseName(), configuration.getTableName());
        this.transactionCount = new AtomicInteger();
        this.stopReadersAndWriters = new AtomicBoolean(false);
        this.failedVerificationCount = new AtomicInteger();
    }

    public void run() throws InterruptedException {
        createDatabaseIfNotExists();
        createOrRecreateTable();

        var numberOfSparkSessionsForWriters = configuration.getNumberOfSparkSessionsForWriters();
        var numberOfSparkSessionsForReaders = configuration.getNumberOfSparkSessionsForReaders();
        var sparkSessionForWriters = createSparkSessions(numberOfSparkSessionsForWriters);
        var sparkSessionForReaders = createSparkSessions(numberOfSparkSessionsForReaders);

        hasFailedReaders = false;
        var numberOfReaderThreads = configuration.getNumberOfReaderThreads();
        var readerThreads = createAndStartReaderThreads(numberOfReaderThreads, sparkSessionForReaders, numberOfSparkSessionsForReaders);

        hasFailedWriters = false;
        var numberOfWriterThreads = configuration.getNumberOfWriterThreads();
        var writerThreads = createAndStartTransactionWriters(numberOfWriterThreads, sparkSessionForWriters, numberOfSparkSessionsForWriters);

        for (var writerThread : writerThreads) {
            writerThread.join();
            hasFailedWriters = hasFailedWriters && writerThread.getWriterException() != null;
        }

        stopReadersAndWriters.set(true);

        for (var readerThread : readerThreads) {
            readerThread.join();
            hasFailedReaders = hasFailedReaders && readerThread.getReaderException() != null;
        }
        System.out.println("ACID Verification finished!");
    }

    private void createDatabaseIfNotExists() {
        session.sql("CREATE DATABASE IF NOT EXISTS " + configuration.getCatalogName() + "." + configuration.getDatabaseName() + ";");
    }

    private void createOrRecreateTable() {
        session.sql("DROP TABLE IF EXISTS " + fullyQualifiedTableName);
        session.sql(String.format("CREATE TABLE IF NOT EXISTS %s(\n" +
                "primaryKeyValue STRING,\n" +
                "partitionKeyValue STRING,\n" +
                "dataValue STRING)\n" +
                "USING iceberg\n" +
                "PARTITIONED BY (partitionKeyValue)\n" +
                "TBLPROPERTIES (\n" +
                "'commit.retry.num-retries' = '100')", fullyQualifiedTableName));
    }

    public boolean hasFailedVerification() {
        return failedVerificationCount.get() > 0;
    }

    private SparkSession[] createSparkSessions(final int numberOfSparkSessions) {
        var childSessions = new SparkSession[numberOfSparkSessions];
        for (var sessionNumber = 0; sessionNumber < numberOfSparkSessions; sessionNumber++) {
            childSessions[sessionNumber] = session.cloneSession();
        }
        return childSessions;
    }

    private ReaderThread[] createAndStartReaderThreads(final int numberOfReaderThreads, final SparkSession[] sessions, final int numberOfSparkSessions) {
        var readerThreads = new ReaderThread[numberOfReaderThreads];
        for (var readerNumber = 0; readerNumber < numberOfReaderThreads; readerNumber++) {
            var childSession = sessions[readerNumber % numberOfSparkSessions];
            readerThreads[readerNumber] = new ReaderThread(transactionLog, childSession, fullyQualifiedTableName, stopReadersAndWriters, this::failedVerificationCallback);
            readerThreads[readerNumber].setName("acid-reader-" + readerNumber);
            readerThreads[readerNumber].start();
        }
        return readerThreads;
    }

    private TransactionWriter[] createAndStartTransactionWriters(final int numberOfWriterThreads, final SparkSession[] sessions, final int numberOfSparkSessions) {
        var writerThreads = new TransactionWriter[numberOfWriterThreads];
        for (var writerNumber = 0; writerNumber < numberOfWriterThreads; writerNumber++) {
            var childSession = sessions[writerNumber % numberOfSparkSessions];
            writerThreads[writerNumber] = new TransactionWriter(
                    transactionLog,
                    this::provideTransactionIfLimitNotReached,
                    transactionGenerator::transactionCommitted,
                    childSession,
                    fullyQualifiedTableName,
                    stopReadersAndWriters
            );
            writerThreads[writerNumber].setName("acid-writer-" + writerNumber);
            writerThreads[writerNumber].start();
        }
        return writerThreads;
    }

    private Transaction provideTransactionIfLimitNotReached() {
        final var transactionNumber = transactionCount.incrementAndGet();
        if (transactionNumber <= configuration.getTotalNumberOfTransactions()) {
            return transactionGenerator.getNextTransaction();
        } else {
            stopReadersAndWriters.set(true);
            return null;
        }
    }

    private void failedVerificationCallback() {
        this.failedVerificationCount.incrementAndGet();
        this.stopReadersAndWriters.set(true);
    }
}
