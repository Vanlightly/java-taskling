package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class TaskExecutionCompleteRequest extends RequestBase {
    public TaskExecutionCompleteRequest(TaskId taskId, String taskExecutionId, String executionTokenId)
    {
        super(taskId, taskExecutionId);
        this.executionTokenId = executionTokenId;
    }

    private String executionTokenId;
    private boolean failed;

    public String getExecutionTokenId() {
        return executionTokenId;
    }

    public void setExecutionTokenId(String executionTokenId) {
        this.executionTokenId = executionTokenId;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }
}
