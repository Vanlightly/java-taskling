package com.siiconcatel.taskling.core.tasks;

import java.time.Instant;

public class TaskExecutionMetaWithHeader<T> {
    private Instant startedAt;
    private Instant completedAt;
    private TaskExecutionStatus status;
    private String referenceValue;
    private T header;

    public TaskExecutionMetaWithHeader(Instant startedAt, Instant completedAt, TaskExecutionStatus status, String referenceValue, T header) {
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.status = status;
        this.referenceValue = referenceValue;
        this.header = header;
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

    public T getHeader() {
        return header;
    }
}
