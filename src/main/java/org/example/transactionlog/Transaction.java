package org.example.transactionlog;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.UUID;

@EqualsAndHashCode
@ToString
public class Transaction {
    public final UUID transactionId;
    public final ManipulationType manipulationType;
    public final List<DataManipulation> dataManipulations;

    public Transaction(final ManipulationType manipulationType, final List<DataManipulation> dataManipulations) {
        this.transactionId = UUID.randomUUID();
        this.manipulationType = manipulationType;
        this.dataManipulations = dataManipulations;
    }

}
