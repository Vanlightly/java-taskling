package com.siiconcatel.taskling.core.configuration;

public class TaskConfigurationException extends RuntimeException {
    public TaskConfigurationException(String message)
    {
        super(message);
    }

    public TaskConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
