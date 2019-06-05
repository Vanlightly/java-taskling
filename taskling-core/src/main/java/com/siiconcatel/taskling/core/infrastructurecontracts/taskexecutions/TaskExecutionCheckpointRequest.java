package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class TaskExecutionCheckpointRequest extends RequestBase {
    private String message;

    public TaskExecutionCheckpointRequest(TaskId taskId, String taskExecutionId, String message) {
        super(taskId, taskExecutionId);
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
