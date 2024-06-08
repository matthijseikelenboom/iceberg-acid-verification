package org.example.writer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.example.resultset.InconsistentResultSetException;
import org.example.resultset.Record;
import org.example.transactionlog.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class TransactionWriter extends Thread {
    private final TransactionLog transactionLog;
    private final Supplier<Transaction> transactionSupplier;
    private final Consumer<Transaction> transactionCommittedConsumer;
    private final SparkSession session;
    private final String fullyQualifiedTableName;
    private final AtomicInteger tempViewNumber;
    private final AtomicBoolean stopWriter;

    @Getter
    private Exception writerException;

    public TransactionWriter(
            TransactionLog transactionLog,
            Supplier<Transaction> transactionSupplier,
            Consumer<Transaction> transactionCommittedConsumer,
            SparkSession session,
            String fullyQualifiedTableName,
            AtomicBoolean stopWriter
    ) {
        this.transactionLog = transactionLog;
        this.transactionSupplier = transactionSupplier;
        this.transactionCommittedConsumer = transactionCommittedConsumer;
        this.session = session;
        this.fullyQualifiedTableName = fullyQualifiedTableName;
        this.tempViewNumber = new AtomicInteger();
        this.stopWriter = stopWriter;
    }

    @Override
    public void run() {
        log.info("{} started.", Thread.currentThread().getName());
        try {
            while (!stopWriter.get()) {
                var transaction = transactionSupplier.get();
                if (transaction != null) {
                    log.info("[Writer] Handling transaction {}", transaction.transactionId);
                    handleTransaction(transaction);
                }
            }
        } catch (Exception e) {
            log.error("Exception in writer.", e);
            writerException = e;
        }
        log.info("{} finished.", Thread.currentThread().getName());
    }

    private void handleTransaction(final Transaction transaction) {
        transactionLog.add(new TransactionLogEvent(EventType.TRANSACTION_INTENDED, transaction));
        var timeBeforeTransaction = System.currentTimeMillis();
        withRetryOnException(() -> {
            switch (transaction.manipulationType) {
                case INSERT:
                    insertTransaction(transaction);
                    break;
                case UPDATE:
                    updateTransaction(transaction);
                    break;
                case DELETE:
                    deleteTransaction(transaction);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown manipulationType: " + transaction.manipulationType);
            }
        });
        var transactionDuration = System.currentTimeMillis() - timeBeforeTransaction;
        log.info("Acid Verification threadType='writer' manipulationType={} duration={}", transaction.manipulationType, transactionDuration);
        transactionCommittedConsumer.accept(transaction);
        transactionLog.logCommit(transaction);
    }

    private void withRetryOnException(DataManipulationTransaction transaction) {
        var retryCount = 0;
        var ranSuccessfully = false;
        while (!ranSuccessfully) {
            try {
                transaction.run();
                ranSuccessfully = true;
            } catch (Throwable e) {
                log.error("Transaction failed.", e);
                if (retryCount >= 100) {
                    throw new RuntimeException(e);
                }
                retryCount++;
            }
        }
    }

    private void insertTransaction(Transaction transaction) {
        final var records = transaction.dataManipulations
                .stream()
                .map(TransactionWriter::mapToRecord)
                .collect(Collectors.toList());
        final var dataSet = session.createDataset(records, Record.getEncoder());

        try {
            dataSet.writeTo(fullyQualifiedTableName).append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateTransaction(Transaction transaction) throws InconsistentResultSetException {
        final var records = transaction.dataManipulations
                .stream()
                .map(TransactionWriter::mapToRecord)
                .collect(Collectors.toList());

        try {
            var rowsToUpdate = session.createDataset(records, Record.getEncoder());
            var tempViewName = "temp_view_" + tempViewNumber.incrementAndGet();
            rowsToUpdate.createTempView(tempViewName);
            var updateStatement = "MERGE INTO " + fullyQualifiedTableName + " t \n" +
                    "USING (SELECT * FROM " + tempViewName +") s \n" +
                    "ON t.primaryKeyValue = s.primaryKeyValue \n" +
                    "WHEN MATCHED THEN UPDATE SET t.dataValue = s.dataValue " +
                    "WHEN NOT MATCHED THEN " +
                    "INSERT (t.primaryKeyValue, t.partitionKeyValue, t.dataValue) VALUES (s.primaryKeyValue, s.partitionKeyValue, s.dataValue);";
            System.out.println(updateStatement);
            session.sql(updateStatement);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteTransaction(Transaction transaction) throws InconsistentResultSetException {
        var primaryKeyValues = transaction.dataManipulations
                .stream()
               .map(dataManipulation -> dataManipulation.primaryKeyValue)
               .collect(Collectors.joining("', '", "'", "'"));

        var deleteStatement = String.format("DELETE FROM %s WHERE primaryKeyValue IN (%s)", fullyQualifiedTableName, primaryKeyValues);
        System.out.println(deleteStatement);
        session.sql(deleteStatement);
    }

    private static Record mapToRecord(DataManipulation dataManipulation) {
        return new Record(dataManipulation.primaryKeyValue, dataManipulation.partitionKeyValue, dataManipulation.dataValue);
    }

    private interface DataManipulationTransaction {
        void run() throws SparkException;
    }
}
