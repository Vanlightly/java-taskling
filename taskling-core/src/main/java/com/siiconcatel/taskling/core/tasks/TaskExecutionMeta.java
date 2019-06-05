package com.siiconcatel.taskling.core.tasks;

import java.time.Instant;

public class TaskExecutionMeta {
    private Instant startedAt;
    private Instant completedAt;
    private TaskExecutionStatus status;
    private String referenceValue;

    public TaskExecutionMeta(Instant startedAt, Instant completedAt, TaskExecutionStatus status, String referenceValue) {
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.status = status;
        this.referenceValue = referenceValue;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public TaskExecutionStatus getStatus() {
        return status;
    }

    public String getReferenceValue() {
        return referenceValue;
    }
}
