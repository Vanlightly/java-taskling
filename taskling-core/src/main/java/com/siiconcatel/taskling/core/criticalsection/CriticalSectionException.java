package com.siiconcatel.taskling.core.criticalsection;

public class CriticalSectionException extends RuntimeException {
    public CriticalSectionException(String message) {
        super(message);
    }

    public CriticalSectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
