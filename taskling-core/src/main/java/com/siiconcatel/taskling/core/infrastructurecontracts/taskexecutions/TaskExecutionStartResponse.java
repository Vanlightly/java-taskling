package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.ResponseBase;

import java.time.Instant;

public class TaskExecutionStartResponse extends ResponseBase{
    public TaskExecutionStartResponse()
    { }

    public TaskExecutionStartResponse(String executionTokenId,
                                      Instant startedAt,
                                      GrantStatus grantStatus)
    {
        this.executionTokenId = executionTokenId;
        this.startedAt = startedAt;
        this.grantStatus = grantStatus;
    }

    private String taskExecutionId;
    private String executionTokenId;
    private Instant startedAt;
    private GrantStatus grantStatus;
    private Exception ex;

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        this.taskExecutionId = taskExecutionId;
    }

    public String getExecutionTokenId() {
        return executionTokenId;
    }

    public void setExecutionTokenId(String executionTokenId) {
        this.executionTokenId = executionTokenId;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public GrantStatus getGrantStatus() {
        return grantStatus;
    }

    public void setGrantStatus(GrantStatus grantStatus) {
        this.grantStatus = grantStatus;
    }

    public Exception getEx() {
        return ex;
    }

    public void setEx(Exception ex) {
        this.ex = ex;
    }
}
