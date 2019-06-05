package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

import java.time.Instant;

public interface TaskRepository {
    TaskDefinition ensureTaskDefinition(TaskId taskId);
    Instant getLastTaskCleanUpTime(TaskId taskId);
    void setLastCleaned(TaskId taskId);
}
