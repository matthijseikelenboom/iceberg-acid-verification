package org.example.transactionlog;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Objects;

@EqualsAndHashCode
@ToString
public class TransactionLogEvent {
    public final EventType eventType;
    public final Transaction transaction;

    public TransactionLogEvent(final EventType eventType, final Transaction transaction) {
        Objects.requireNonNull(eventType);
        Objects.requireNonNull(transaction);
        this.eventType = eventType;
        this.transaction = transaction;
    }
}
