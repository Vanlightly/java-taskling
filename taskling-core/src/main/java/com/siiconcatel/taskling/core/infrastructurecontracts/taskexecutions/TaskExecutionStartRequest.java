package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;

import java.time.Duration;
import java.util.Optional;

public class TaskExecutionStartRequest extends RequestBase {
    public TaskExecutionStartRequest(TaskId taskId,
                                     TaskDeathMode taskDeathMode,
                                     int concurrencyLimit,
                                     int failedTaskRetryLimit,
                                     int deadTaskRetryLimit
    )
    {
        super(taskId);
        this.taskDeathMode = taskDeathMode;
        this.concurrencyLimit = concurrencyLimit;
        this.failedTaskRetryLimit = failedTaskRetryLimit;
        this.deadTaskRetryLimit = deadTaskRetryLimit;
    }

    private String tasklingVersion;
    private TaskDeathMode taskDeathMode;
    private Duration overrideThreshold;
    private Duration keepAliveInterval;
    private Duration keepAliveDeathThreshold;
    private String referenceValue;
    private int concurrencyLimit;
    private int failedTaskRetryLimit;
    private int deadTaskRetryLimit;
    private String taskExecutionHeader;

    public String getTasklingVersion() {
        return tasklingVersion;
    }

    public void setTasklingVersion(String tasklingVersion) {
        this.tasklingVersion = tasklingVersion;
    }

    public TaskDeathMode getTaskDeathMode() {
        return taskDeathMode;
    }

    public void setTaskDeathMode(TaskDeathMode taskDeathMode) {
        this.taskDeathMode = taskDeathMode;
    }

    public Optional<Duration> getOverrideThreshold() {

        if(overrideThreshold == null)
            return Optional.empty();

        return Optional.of(overrideThreshold);
    }

    public void setOverrideThreshold(Duration overrideThreshold) {
        this.overrideThreshold = overrideThreshold;
    }

    public Optional<Duration> getKeepAliveInterval() {

        if(keepAliveInterval == null)
            return Optional.empty();

        return Optional.of(keepAliveInterval);
    }

    public void setKeepAliveInterval(Duration keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public Optional<Duration> getKeepAliveDeathThreshold() {

        if(keepAliveDeathThreshold == null)
            return Optional.empty();

        return Optional.of(keepAliveDeathThreshold);
    }

    public void setKeepAliveDeathThreshold(Duration keepAliveDeathThreshold) {
        this.keepAliveDeathThreshold = keepAliveDeathThreshold;
    }

    public String getReferenceValue() {
        return referenceValue;
    }

    public void setReferenceValue(String referenceValue) {
        this.referenceValue = referenceValue;
    }

    public int getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public int getFailedTaskRetryLimit() {
        return failedTaskRetryLimit;
    }

    public void setFailedTaskRetryLimit(int failedTaskRetryLimit) {
        this.failedTaskRetryLimit = failedTaskRetryLimit;
    }

    public int getDeadTaskRetryLimit() {
        return deadTaskRetryLimit;
    }

    public void setDeadTaskRetryLimit(int deadTaskRetryLimit) {
        this.deadTaskRetryLimit = deadTaskRetryLimit;
    }

    public String getTaskExecutionHeader() {
        return taskExecutionHeader;
    }

    public void setTaskExecutionHeader(String taskExecutionHeader) {
        this.taskExecutionHeader = taskExecutionHeader;
    }
}
