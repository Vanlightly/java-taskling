package com.siiconcatel.taskling.core.infrastructurecontracts.cleanup;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

import java.time.Duration;
import java.time.Instant;

public class CleanUpRequest extends RequestBase {
    private Instant generalDateThreshold;
    private Instant listItemDateThreshold;
    private Duration timeSinceLastCleaningThreashold;

    public CleanUpRequest(TaskId taskId, String taskExecutionId, Instant generalDateThreshold, Instant listItemDateThreshold, Duration timeSinceLastCleaningThreashold) {
        super(taskId, taskExecutionId);
        this.generalDateThreshold = generalDateThreshold;
        this.listItemDateThreshold = listItemDateThreshold;
        this.timeSinceLastCleaningThreashold = timeSinceLastCleaningThreashold;
    }

    public Instant getGeneralDateThreshold() {
        return generalDateThreshold;
    }

    public void setGeneralDateThreshold(Instant generalDateThreshold) {
        this.generalDateThreshold = generalDateThreshold;
    }

    public Instant getListItemDateThreshold() {
        return listItemDateThreshold;
    }

    public void setListItemDateThreshold(Instant listItemDateThreshold) {
        this.listItemDateThreshold = listItemDateThreshold;
    }

    public Duration getTimeSinceLastCleaningThreashold() {
        return timeSinceLastCleaningThreashold;
    }

    public void setTimeSinceLastCleaningThreashold(Duration timeSinceLastCleaningThreashold) {
        this.timeSinceLastCleaningThreashold = timeSinceLastCleaningThreashold;
    }
}
