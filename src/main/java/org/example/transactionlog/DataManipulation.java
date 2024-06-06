package org.example.transactionlog;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Objects;

@EqualsAndHashCode
@ToString
public class DataManipulation {
    public final String primaryKeyValue;
    public final String partitionKeyValue;
    public final String dataValue;

    public DataManipulation(final String primaryKeyValue, final String partitionKeyValue, final String dataValue) {
        Objects.requireNonNull(primaryKeyValue);
        Objects.requireNonNull(partitionKeyValue);
        this.primaryKeyValue = primaryKeyValue;
        this.partitionKeyValue = partitionKeyValue;
        this.dataValue = dataValue;
    }

    public DataManipulation(final String primaryKeyValue, final String partitionKeyValue) {
        Objects.requireNonNull(primaryKeyValue);
        Objects.requireNonNull(partitionKeyValue);
        this.primaryKeyValue = primaryKeyValue;
        this.partitionKeyValue = partitionKeyValue;
        this.dataValue = "";
    }
}
