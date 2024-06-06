package org.example;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.example.resultset.Record;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestDataFactory {

    public static Record createRecord(int sequenceNumber) {
        var primaryKeyValue = "PK" + sequenceNumber;
        var partitionKeyValue = "Partition" + (sequenceNumber % 4);
        var dataValue = "Initial value " + sequenceNumber;
        return new Record(primaryKeyValue, partitionKeyValue, dataValue);
     }

     public static Record createUpdatedRecord(Record recordToBeUpdated) {
        return new Record(recordToBeUpdated.getPrimaryKeyValue(), recordToBeUpdated.getPartitionKeyValue(), recordToBeUpdated.getDataValue() + " updated");
     }

}
