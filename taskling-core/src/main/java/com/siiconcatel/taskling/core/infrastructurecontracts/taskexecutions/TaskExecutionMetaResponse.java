package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import java.util.ArrayList;
import java.util.List;

public class TaskExecutionMetaResponse {
    public TaskExecutionMetaResponse()
    {
        this.executions = new ArrayList<>();
    }

    private List<TaskExecutionMetaItem> executions;

    public List<TaskExecutionMetaItem> getExecutions() {
        return executions;
    }

    public void setExecutions(List<TaskExecutionMetaItem> executions) {
        this.executions = executions;
    }
}
