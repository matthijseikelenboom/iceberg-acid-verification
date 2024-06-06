package org.example.writer;

public class TransactionFailedException extends RuntimeException {
    public TransactionFailedException(final Throwable cause) {
        super(cause);
    }
}
