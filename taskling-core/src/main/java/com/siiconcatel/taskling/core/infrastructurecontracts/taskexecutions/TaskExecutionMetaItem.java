package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.tasks.TaskExecutionStatus;

import java.time.Instant;
import java.util.Optional;

public class TaskExecutionMetaItem {
    private Instant startedAt;
    private Instant completedAt;
    private TaskExecutionStatus status;
    private String header;
    private String referenceValue;

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Optional<Instant> getCompletedAt() {
        if(completedAt == null)
            return Optional.empty();

        return Optional.of(completedAt);
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public TaskExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(TaskExecutionStatus status) {
        this.status = status;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getReferenceValue() {
        return referenceValue;
    }

    public void setReferenceValue(String referenceValue) {
        this.referenceValue = referenceValue;
    }
}
