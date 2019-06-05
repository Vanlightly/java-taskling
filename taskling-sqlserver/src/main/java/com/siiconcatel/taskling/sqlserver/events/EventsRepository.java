package com.siiconcatel.taskling.sqlserver.events;

import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public interface EventsRepository {
    void logEvent(TaskId taskId, String taskExecutionId, EventType eventType, String message);
}
