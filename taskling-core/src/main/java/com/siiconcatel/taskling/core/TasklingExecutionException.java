package com.siiconcatel.taskling.core;

public class TasklingExecutionException extends RuntimeException {
    public TasklingExecutionException(String message)
    {
        super(message);
    }

    public TasklingExecutionException(String message, Exception innerException)
    {
        super(message, innerException);
    }
}
