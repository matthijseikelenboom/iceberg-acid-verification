package org.example.reader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.example.resultexpectations.ResultSetExpectationProducer;
import org.example.resultset.Record;
import org.example.resultset.ResultSet;
import org.example.transactionlog.TransactionLog;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ReaderThread extends Thread {
    private final TransactionLog transactionLog;
    private final SparkSession session;
    private final String fullyQualifiedTableName;
    private final AtomicBoolean stopReader;
    private final ResultSetExpectationProducer resultSetExpectationProducer;
    private final Runnable verificationFailedCallback;

    @Getter
    private Exception readerException;

    public ReaderThread(
            TransactionLog transactionLog,
            SparkSession session,
            String fullyQualifiedTableName,
            AtomicBoolean stopReader,
            Runnable verificationFailedCallback
    ) {
        this.transactionLog = transactionLog;
        this.session = session;
        this.fullyQualifiedTableName = fullyQualifiedTableName;
        this.stopReader = stopReader;
        this.resultSetExpectationProducer = new ResultSetExpectationProducer(transactionLog);
        this.verificationFailedCallback = verificationFailedCallback;
    }

    @Override
    public void run() {
        try {
            while (!stopReader.get()) {
                performVerification();
            }
        } catch (Exception e) {
            log.error("Exception in reader.", e);
            readerException = e;
        }
    }

    private void performVerification() {
        final var eventCountBeforeRead = transactionLog.getEventCount();
        final var timeBeforeRead = System.currentTimeMillis();
        final var resultSet = readData();
        final var readDuration = System.currentTimeMillis() - timeBeforeRead;
        final var eventCountAfterRead = transactionLog.getEventCount();
        final var resultSetExpectations = resultSetExpectationProducer.createResultSetExpectations(eventCountBeforeRead, eventCountAfterRead);
        final var satisfied = resultSetExpectations.isStatisfied(resultSet);
        if (!satisfied) {
            log.error("Verification Failed. ResultSet:\n{}", resultSet);
            verificationFailedCallback.run();
        }
        log.info(
                "Acid Verification threadType='reader' satisfied='{}' duration={} eventCountBeforeRead={} eventCountAfterRead={} resultSetSize={}",
                satisfied,
                readDuration,
                eventCountBeforeRead,
                eventCountAfterRead,
                resultSet.getRecords().size()
        );
    }

    public ResultSet readData() {
        var recordDataSet = session
                .sql("SELECT * FROM " + fullyQualifiedTableName)
                .as(Record.getEncoder())
                .collectAsList();

        return new ResultSet(recordDataSet);
    }
}
