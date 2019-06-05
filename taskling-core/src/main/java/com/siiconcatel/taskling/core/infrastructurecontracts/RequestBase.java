package com.siiconcatel.taskling.core.infrastructurecontracts;

public class RequestBase
{
    public RequestBase()
    { }

    public RequestBase(TaskId taskId)
    {
        this.taskId = taskId;
    }

    public RequestBase(TaskId taskId, String taskExecutionId)
    {
        this.taskId = taskId;
        this.taskExecutionId = taskExecutionId;
    }

    public TaskId taskId;
    public String taskExecutionId;

    public TaskId getTaskId() {
        return taskId;
    }

    public void setTaskId(TaskId taskId) {
        this.taskId = taskId;
    }

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        this.taskExecutionId = taskExecutionId;
    }
}
