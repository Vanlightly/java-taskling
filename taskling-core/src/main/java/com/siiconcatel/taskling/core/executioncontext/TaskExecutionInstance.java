package com.siiconcatel.taskling.core.executioncontext;

import java.time.Instant;
import java.util.Optional;

public class TaskExecutionInstance
{
    private String taskExecutionId;
    private String applicationName;
    private String taskName;
    private Instant startedAt;
    private Instant completedAt;
    private String executionTokenId;

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        this.taskExecutionId = taskExecutionId;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

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

    public String getExecutionTokenId() {
        return executionTokenId;
    }

    public void setExecutionTokenId(String executionTokenId) {
        this.executionTokenId = executionTokenId;
    }
}
