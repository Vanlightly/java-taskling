package com.siiconcatel.taskling.sqlserver.taskexecution;

import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;

import java.time.Instant;

public class CachedTaskDefinition {
    private TaskDefinition taskDefinition;
    private Instant cachedAt;

    public CachedTaskDefinition(com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition taskDefinition) {
        this.taskDefinition = taskDefinition;
        this.cachedAt = Instant.now();
    }

    public TaskDefinition getTaskDefinition() {
        return taskDefinition;
    }

    public Instant getCachedAt() {
        return cachedAt;
    }
}
