package com.siiconcatel.taskling.sqlserver.tokens.executions;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class TokenRequest {
    private TaskId TaskId;
    private int TaskDefinitionId;
    private String TaskExecutionId;
    private int ConcurrencyLimit;

    public TokenRequest(com.siiconcatel.taskling.core.infrastructurecontracts.TaskId taskId, int taskDefinitionId, String taskExecutionId) {
        TaskId = taskId;
        TaskDefinitionId = taskDefinitionId;
        TaskExecutionId = taskExecutionId;
    }

    public TokenRequest(com.siiconcatel.taskling.core.infrastructurecontracts.TaskId taskId, int taskDefinitionId, String taskExecutionId, int concurrencyLimit) {
        TaskId = taskId;
        TaskDefinitionId = taskDefinitionId;
        TaskExecutionId = taskExecutionId;
        ConcurrencyLimit = concurrencyLimit;
    }

    public com.siiconcatel.taskling.core.infrastructurecontracts.TaskId getTaskId() {
        return TaskId;
    }

    public void setTaskId(com.siiconcatel.taskling.core.infrastructurecontracts.TaskId taskId) {
        TaskId = taskId;
    }

    public int getTaskDefinitionId() {
        return TaskDefinitionId;
    }

    public void setTaskDefinitionId(int taskDefinitionId) {
        TaskDefinitionId = taskDefinitionId;
    }

    public String getTaskExecutionId() {
        return TaskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        TaskExecutionId = taskExecutionId;
    }

    public int getConcurrencyLimit() {
        return ConcurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        ConcurrencyLimit = concurrencyLimit;
    }
}
