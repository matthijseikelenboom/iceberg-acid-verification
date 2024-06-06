package org.example.writer;

import lombok.NonNull;
import org.example.transactionlog.DataManipulation;
import org.example.transactionlog.ManipulationType;
import org.example.transactionlog.Transaction;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TransactionGenerator {
    private final int maximumNumberOfPartitions;
    private final int recordsPertransaction;
    private final Random randomGenerator;
    private final AvailableRecordIdentifiers availableRecordIdentifiers;
    private final float preferenceToInsertOverOtherManipulation;
    private final float preferenceToUpdateOverDelete;
    
    public TransactionGenerator(@NonNull Configuration configuration) {
        maximumNumberOfPartitions = configuration.getMaximumNumberOfPartitions();
        recordsPertransaction = configuration.getRecordsPerTransaction();
        preferenceToInsertOverOtherManipulation = configuration.getPreferenceToInsertOverOtherManipulations();
        preferenceToUpdateOverDelete = configuration.getPreferenceToUpdateOverDelete();
        randomGenerator = new Random(configuration.getRandomSeed());
        availableRecordIdentifiers = new AvailableRecordIdentifiers(configuration.getMaximumNumberOfRecords());
    }
    
    public Transaction getNextTransaction() {
        synchronized (availableRecordIdentifiers) {
            var enoughExistingRecordsForUpdateOrDelete = availableRecordIdentifiers.getNumberOfExistingRecords() >= recordsPertransaction;
            var enoughExistingRecordsForInsert = availableRecordIdentifiers.getNumberOfNonExistingRecords() >= recordsPertransaction;
            ManipulationType manipulationType;
            if (enoughExistingRecordsForInsert && (!enoughExistingRecordsForUpdateOrDelete || randomInsertDecision())) {
                manipulationType = ManipulationType.INSERT;
            } else if (enoughExistingRecordsForUpdateOrDelete) {
                manipulationType = randomUpdateDecision() ? ManipulationType.UPDATE : ManipulationType.DELETE;
            } else {
                throw new IllegalStateException("Not enough record identifiers available to create the next transaction.");
            }
            final var transaction = createTransaction(manipulationType);
            System.out.println(transaction);
            return transaction;
        }
    }

    public void transactionCommitted(Transaction transaction) {
        synchronized (availableRecordIdentifiers) {
            if (transaction.manipulationType == ManipulationType.DELETE) {
                transaction.dataManipulations.forEach(dataManipulation -> availableRecordIdentifiers.addNonExistingRecordIdentifier(dataManipulation.primaryKeyValue));
            } else {
                transaction.dataManipulations.forEach(dataManipulation -> availableRecordIdentifiers.addExistingRecordIdentifier(dataManipulation.primaryKeyValue));
            }
        }
    }
    
    private boolean randomInsertDecision() {
        return randomGenerator.nextFloat() < preferenceToInsertOverOtherManipulation;
    }
    
    private boolean randomUpdateDecision() {
        return randomGenerator.nextFloat() < preferenceToUpdateOverDelete;
    }
    
    private Transaction createTransaction(final ManipulationType manipulationType) {
        var dataManipulations = IntStream.range(0, recordsPertransaction)
                .mapToObj(r -> availableRecordIdentifiers.pollRecordIdentifier(manipulationType))
                .map(this::createDataManipulation)
                .collect(Collectors.toList());
        return new Transaction(manipulationType, dataManipulations);
    }
    
    private DataManipulation createDataManipulation(String recordIdentifier) {
        int partitionNumber = recordIdentifier.hashCode() % maximumNumberOfPartitions;
        var dataValue = "Some random value: " + randomGenerator.nextLong();
        return new DataManipulation(recordIdentifier, "Partition" + partitionNumber, dataValue);
    }

    private final class AvailableRecordIdentifiers {
        private final List<String> existingRecordIdentifiers;
        private final List<String> nonExistingRecordIdentifiers;
        
        private AvailableRecordIdentifiers(int maximumNumberOfRecords) {
            this.existingRecordIdentifiers = new LinkedList<>();
            this.nonExistingRecordIdentifiers = IntStream.range(0, maximumNumberOfRecords)
                    .mapToObj(recordNr -> "Record" + recordNr)
                    .collect(Collectors.toCollection(LinkedList::new));
        }
        
        private int getNumberOfExistingRecords() {
            return existingRecordIdentifiers.size();
        }
        
        private int getNumberOfNonExistingRecords() {
            return nonExistingRecordIdentifiers.size();
        }
        
        private String pollRecordIdentifier(ManipulationType manipulationType) {
            if (manipulationType == ManipulationType.INSERT) {
                return pollIdentifiersForNonExistingRecords();
            }
            return pollIdentifierForExistingRecord();
        }
        
        private String pollIdentifierForExistingRecord() {
            return pollRecordIdentifier(existingRecordIdentifiers);
        }
        
        private String pollIdentifiersForNonExistingRecords() {
            return pollRecordIdentifier(nonExistingRecordIdentifiers);
        }
        
        private String pollRecordIdentifier(final List<String> recordIdentifiers) {
            var recordCount = recordIdentifiers.size();
            if (recordCount == 0) {
                return null;
            }
            var index = randomGenerator.nextInt(recordCount);
            return recordIdentifiers.remove(index);
        }

        private void addExistingRecordIdentifier(String recordIdentifier) {
            existingRecordIdentifiers.add(recordIdentifier);
        }

        private void addNonExistingRecordIdentifier(String recordIdentifier) {
            nonExistingRecordIdentifiers.add(recordIdentifier);
        }
    }
    
}
