package com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;

import java.time.Duration;
import java.util.Optional;

public class StartCriticalSectionRequest extends RequestBase {
    public StartCriticalSectionRequest(TaskId taskId,
                                       String taskExecutionId,
                                       TaskDeathMode taskDeathMode,
                                       CriticalSectionType criticalSectionType)
    {
        super(taskId, taskExecutionId);
        this.taskDeathMode = taskDeathMode;
        this.criticalSectionType = criticalSectionType;
    }

    private CriticalSectionType criticalSectionType;
    private TaskDeathMode taskDeathMode;
    private Duration overrideThreshold;
    private Duration keepAliveDeathThreshold;

    public CriticalSectionType getCriticalSectionType() {
        return criticalSectionType;
    }

    public void setCriticalSectionType(CriticalSectionType criticalSectionType) {
        this.criticalSectionType = criticalSectionType;
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

    public Optional<Duration> getKeepAliveDeathThreshold() {

        if(keepAliveDeathThreshold == null)
            return Optional.empty();

        return Optional.of(keepAliveDeathThreshold);
    }

    public void setKeepAliveDeathThreshold(Duration keepAliveDeathThreshold) {
        this.keepAliveDeathThreshold = keepAliveDeathThreshold;
    }
}
