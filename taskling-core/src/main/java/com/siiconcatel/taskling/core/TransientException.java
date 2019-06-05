package com.siiconcatel.taskling.core;

public class TransientException extends RuntimeException {
    public TransientException(String message) {
        super(message);
    }

    public TransientException(String message, Exception innerException) {
        super(message, innerException);
    }
}