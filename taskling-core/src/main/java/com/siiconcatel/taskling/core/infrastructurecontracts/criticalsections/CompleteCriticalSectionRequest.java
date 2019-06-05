package com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class CompleteCriticalSectionRequest extends RequestBase {
    public CompleteCriticalSectionRequest(TaskId taskId, String taskExecutionId, CriticalSectionType criticalSectionType)
    {
        super(taskId, taskExecutionId);
        this.criticalSectionType = criticalSectionType;
    }

    private CriticalSectionType criticalSectionType;

    public CriticalSectionType getCriticalSectionType() {
        return criticalSectionType;
    }

    public void setCriticalSectionType(CriticalSectionType criticalSectionType) {
        this.criticalSectionType = criticalSectionType;
    }
}
