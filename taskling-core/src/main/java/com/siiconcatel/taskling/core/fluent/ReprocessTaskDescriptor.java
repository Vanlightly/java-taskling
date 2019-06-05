package com.siiconcatel.taskling.core.fluent;

public interface ReprocessTaskDescriptor {
    /**
     * Instructs the execution context to filter blocks to those
     * previously processed by a task execution with the specified
     * reference value
     * @param referenceValue
     * @return
     */
    CompleteDescriptor ofExecutionWith(String referenceValue);
}
