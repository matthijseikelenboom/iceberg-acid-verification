package org.example.resultset;

public class InconsistentResultSetException extends RuntimeException {
    public InconsistentResultSetException(final String message) {
        super(message);
    }
}
