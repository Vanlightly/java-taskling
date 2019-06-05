package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;

import java.time.Instant;

public class TaskExecutionCompleteResponse extends RequestBase {
    private Instant completedAt;

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }
}
