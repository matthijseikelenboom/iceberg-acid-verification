package org.example.resultexpectations;

import lombok.Getter;
import org.example.resultset.Record;
import org.example.transactionlog.DataManipulation;
import org.example.transactionlog.EventType;
import org.example.transactionlog.TransactionLog;
import org.example.transactionlog.TransactionLogEvent;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class ResultSetExpectationProducer {
    private final TransactionLog transactionLog;

    public ResultSetExpectationProducer(final TransactionLog transactionLog) {
        this.transactionLog = transactionLog;
    }

    public ResultSetExpectations createResultSetExpectations(int eventCountBeforeRead, int eventCountAfterRead) {
        var expectations = new ResultSetExpectations();
        var eventFilter = new EventFilter(eventCountBeforeRead, eventCountAfterRead);
        for (var event : eventFilter.getIntendedEvents()) {
            var isCommittedBeforeRead = eventFilter.isCommittedBeforeRead(event);
            for (var dataManipulation : event.transaction.dataManipulations) {
                Expectation expectation = null;
                switch (event.transaction.manipulationType) {
                    case INSERT:
                        expectation = createInsertExpectation(expectations, dataManipulation, isCommittedBeforeRead);
                        break;
                    case UPDATE:
                        expectation = createUpdateExpectation(expectations, dataManipulation, isCommittedBeforeRead);
                        break;
                    case DELETE:
                        expectation = createDeleteExpectation(expectations, dataManipulation, isCommittedBeforeRead);
                        break;
                    default:
                        throw new IllegalStateException("Kaput!");
                }
                expectations.setRecordExpectation(dataManipulation.primaryKeyValue, expectation);
            }
        }
        return expectations;
    }

    private static Expectation createInsertExpectation(final ResultSetExpectations expectations, final DataManipulation dataManipulation, final boolean isCommittedBeforeRead) {
        var insertedRecord = new Record(dataManipulation.primaryKeyValue, dataManipulation.partitionKeyValue, dataManipulation.dataValue);
        var insertSucceededExpectation = ExpectRecordPresence.create(insertedRecord);
        if (isCommittedBeforeRead) {
            return insertSucceededExpectation;
        } else {
            var insertNotHappenedYetExpectation = expectations.getRecordExpectation(dataManipulation.primaryKeyValue)
                    .orElse(ExpectRecordAbsence.create(insertedRecord));
            return insertNotHappenedYetExpectation.or(insertSucceededExpectation);
        }
    }

    private static Expectation createUpdateExpectation(final ResultSetExpectations expectations, final DataManipulation dataManipulation, final boolean isCommittedBeforeRead) {
        var updatedRecord = new Record(dataManipulation.primaryKeyValue, dataManipulation.partitionKeyValue, dataManipulation.dataValue);
        var updateSucceededExpectation = ExpectRecordPresence.create(updatedRecord);
        if (isCommittedBeforeRead) {
            return updateSucceededExpectation;
        } else {
            var updateNotHappenedYetExpectation = expectations.getRecordExpectation(dataManipulation.primaryKeyValue)
                    .orElseThrow(() -> new IllegalStateException("When updating a record, there should already be an expectation on the existing record"));
            return updateNotHappenedYetExpectation.or(updateSucceededExpectation);
        }
    }

    private static Expectation createDeleteExpectation(final ResultSetExpectations expectations, final DataManipulation dataManipulation, final boolean isCommittedBeforeRead) {
        var recordToDelete = new Record(dataManipulation.primaryKeyValue, dataManipulation.partitionKeyValue, dataManipulation.dataValue);
        var deleteSucceededExpectation = ExpectRecordAbsence.create(recordToDelete);
        if (isCommittedBeforeRead) {
            return deleteSucceededExpectation;
        } else {
            var deleteNotHappenedYetExpectation = expectations.getRecordExpectation(dataManipulation.primaryKeyValue)
                    .orElseThrow(() -> new IllegalStateException("When deleting a record, there should already be an expectation on the existing record"));
            return deleteNotHappenedYetExpectation.or(deleteSucceededExpectation);
        }
    }

    public class EventFilter {
        @Getter
        private final List<TransactionLogEvent> intendedEvents;
        private final Set<UUID> committedTransactionsBeforeRead;

        EventFilter(int eventCountBeforeRead, int eventCountAfterRead) {
            var events = transactionLog.getFirstNEvents(eventCountAfterRead);
            this.intendedEvents = events.stream()
                    .filter(event -> event.eventType == EventType.TRANSACTION_INTENDED)
                    .collect(Collectors.toList());
            this.committedTransactionsBeforeRead = events.stream()
                    .limit(eventCountBeforeRead)
                    .filter(event -> event.eventType == EventType.TRANSACTION_COMMITTED)
                    .map(event -> event.transaction.transactionId)
                    .collect(Collectors.toSet());
        }

        boolean isCommittedBeforeRead(TransactionLogEvent event) {
            return committedTransactionsBeforeRead.contains(event.transaction.transactionId);
        }
    }

}
