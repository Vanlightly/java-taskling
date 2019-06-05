package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class TaskExecutionMetaRequest extends RequestBase {
    private int executionsToRetrieve;

    public TaskExecutionMetaRequest(TaskId taskId, int executionsToRetrieve) {
        super(taskId);
        this.executionsToRetrieve = executionsToRetrieve;
    }

    public int getExecutionsToRetrieve() {
        return executionsToRetrieve;
    }

    public void setExecutionsToRetrieve(int executionsToRetrieve) {
        this.executionsToRetrieve = executionsToRetrieve;
    }
}
