package com.siiconcatel.taskling.core.configuration;

public interface TasklingConfiguration {
    TaskConfig getTaskConfiguration(String applicationName, String taskName);
}
