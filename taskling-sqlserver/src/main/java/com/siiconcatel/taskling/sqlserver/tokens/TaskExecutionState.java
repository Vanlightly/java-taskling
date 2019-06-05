package com.siiconcatel.taskling.sqlserver.tokens;

import com.siiconcatel.taskling.core.tasks.TaskDeathMode;

import java.time.Duration;
import java.time.Instant;

public class TaskExecutionState
{
    private String taskExecutionId;
    private Instant startedAt;
    private Instant completedAt;
    private Instant lastKeepAlive;
    private TaskDeathMode taskDeathMode;
    private Duration overrideThreshold;
    private Duration keepAliveInterval;
    private Duration keepAliveDeathThreshold;
    private Instant currentDateTime;
    private int queueIndex;

    public TaskExecutionState() {}

    public TaskExecutionState(String taskExecutionId, Instant startedAt, Instant completedAt, Instant lastKeepAlive, TaskDeathMode taskDeathMode, Duration overrideThreshold, Duration keepAliveInterval, Duration keepAliveDeathThreshold, Instant currentDateTime, int queueIndex) {
        this.taskExecutionId = taskExecutionId;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.lastKeepAlive = lastKeepAlive;
        this.taskDeathMode = taskDeathMode;
        this.overrideThreshold = overrideThreshold;
        this.keepAliveInterval = keepAliveInterval;
        this.keepAliveDeathThreshold = keepAliveDeathThreshold;
        this.currentDateTime = currentDateTime;
        this.queueIndex = queueIndex;
    }

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        this.taskExecutionId = taskExecutionId;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public Instant getLastKeepAlive() {
        return lastKeepAlive;
    }

    public void setLastKeepAlive(Instant lastKeepAlive) {
        this.lastKeepAlive = lastKeepAlive;
    }

    public TaskDeathMode getTaskDeathMode() {
        return taskDeathMode;
    }

    public void setTaskDeathMode(TaskDeathMode taskDeathMode) {
        this.taskDeathMode = taskDeathMode;
    }

    public Duration getOverrideThreshold() {
        return overrideThreshold;
    }

    public void setOverrideThreshold(Duration overrideThreshold) {
        this.overrideThreshold = overrideThreshold;
    }

    public Duration getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(Duration keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public Duration getKeepAliveDeathThreshold() {
        return keepAliveDeathThreshold;
    }

    public void setKeepAliveDeathThreshold(Duration keepAliveDeathThreshold) {
        this.keepAliveDeathThreshold = keepAliveDeathThreshold;
    }

    public Instant getCurrentDateTime() {
        return currentDateTime;
    }

    public void setCurrentDateTime(Instant currentDateTime) {
        this.currentDateTime = currentDateTime;
    }

    public int getQueueIndex() {
        return queueIndex;
    }

    public void setQueueIndex(int queueIndex) {
        this.queueIndex = queueIndex;
    }
}
