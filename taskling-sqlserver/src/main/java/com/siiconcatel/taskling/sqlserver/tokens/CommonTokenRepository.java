package com.siiconcatel.taskling.sqlserver.tokens;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface CommonTokenRepository {
    void acquireRowLock(int taskDefinitionId, String taskExecutionId, Connection connection) throws SQLException;
    List<TaskExecutionState> getTaskExecutionStates(List<String> taskExecutionIds, Connection connection) throws SQLException;
    boolean hasExpired(TaskExecutionState taskExecutionState);
}
