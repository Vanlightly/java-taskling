package com.siiconcatel.taskling.core.cleanup;

public interface CleanUpService {
    void cleanOldData(String applicationName, String taskName, String taskExecutionId);
}
