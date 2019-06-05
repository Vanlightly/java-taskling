package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class TaskExecutionErrorRequest extends RequestBase {
    private String error;
    private boolean treatTaskAsFailed;

    public TaskExecutionErrorRequest(TaskId taskId, String taskExecutionId, String error, boolean treatTaskAsFailed) {
        super(taskId, taskExecutionId);
        this.error = error;
        this.treatTaskAsFailed = treatTaskAsFailed;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public boolean isTreatTaskAsFailed() {
        return treatTaskAsFailed;
    }

    public void setTreatTaskAsFailed(boolean treatTaskAsFailed) {
        this.treatTaskAsFailed = treatTaskAsFailed;
    }
}
