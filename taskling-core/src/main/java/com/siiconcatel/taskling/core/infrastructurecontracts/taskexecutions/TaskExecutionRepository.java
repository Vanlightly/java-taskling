package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

public interface TaskExecutionRepository {
    TaskExecutionStartResponse start(TaskExecutionStartRequest startRequest);
    TaskExecutionCompleteResponse complete(TaskExecutionCompleteRequest completeRequest);
    void checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest);
    void error(TaskExecutionErrorRequest taskExecutionErrorRequest);
    void sendKeepAlive(SendKeepAliveRequest sendKeepAliveRequest);
    TaskExecutionMetaResponse getLastExecutionMetas(TaskExecutionMetaRequest taskExecutionMetaRequest);
}
