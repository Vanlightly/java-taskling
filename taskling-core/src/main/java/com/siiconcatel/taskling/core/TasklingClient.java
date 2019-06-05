package com.siiconcatel.taskling.core;

import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;

/**
 * Creates TaskExecutionContexts
 */
public interface TasklingClient {

    /**
     * Returns a TaskExecutionContext
     * @param   applicationName The application name component of the task identifier
     * @param   taskName        The task name component of the task identifier
     * @return A TaskExecutionContext with the configuration of the applicationName + taskName identifier
     */
    TaskExecutionContext createTaskExecutionContext(String applicationName, String taskName);
}
