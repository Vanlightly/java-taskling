package com.siiconcatel.taskling.sqlserver.tokens;

import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NullableField;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class CommonTokenRepositoryMsSql implements CommonTokenRepository {

    public void acquireRowLock(int taskDefinitionId, String taskExecutionId, Connection connection) throws SQLException
    {
        NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTokens.AcquireLockQuery);
        p.setInt("taskDefinitionId", taskDefinitionId);
        p.setLong("taskExecutionId", Long.parseLong(taskExecutionId));
        p.execute();
    }

    public List<TaskExecutionState> getTaskExecutionStates(List<String> taskExecutionIds, Connection connection) throws SQLException
    {
        List<TaskExecutionState> results = new ArrayList<>();

        NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTokens.GetTaskExecutions(taskExecutionIds.size()));
        for (int i = 0; i < taskExecutionIds.size(); i++)
            p.setInt("inParam" + i, Integer.parseInt(taskExecutionIds.get(i)));

        ResultSet rs = p.executeQuery();
        while(rs.next()) {
            TaskExecutionState teState = new TaskExecutionState();
            teState.setCompletedAt(NullableField.getInstant(rs, "CompletedAt"));
            teState.setKeepAliveDeathThreshold(NullableField.getDuration(rs, "KeepAliveDeathThreshold"));
            teState.setKeepAliveInterval(NullableField.getDuration(rs, "KeepAliveInterval"));
            teState.setLastKeepAlive(NullableField.getInstant(rs, "LastKeepAlive"));
            teState.setOverrideThreshold(NullableField.getDuration(rs, "OverrideThreshold"));
            teState.setStartedAt(TimeHelper.toInstant(rs.getTimestamp("StartedAt")));
            teState.setTaskDeathMode(TaskDeathMode.valueOf(rs.getInt("TaskDeathMode")));
            teState.setTaskExecutionId(rs.getString("TaskExecutionId"));
            teState.setCurrentDateTime(TimeHelper.toInstant(rs.getTimestamp("CurrentDateTime")));

            results.add(teState);
        }

        return results;
    }

    public boolean hasExpired(TaskExecutionState taskExecutionState)
    {
        if (taskExecutionState.getCompletedAt() != null)
            return true;

        if (taskExecutionState.getTaskDeathMode() == TaskDeathMode.KeepAlive)
        {
            if (taskExecutionState.getLastKeepAlive() == null)
                return true;

            Duration lastKeepAliveDiff = Duration.between(taskExecutionState.getLastKeepAlive(), taskExecutionState.getCurrentDateTime());
            if (lastKeepAliveDiff.compareTo(taskExecutionState.getKeepAliveDeathThreshold()) > 0)
                return true;

            return false;
        }
        else
        {
            Duration activePeriod = Duration.between(taskExecutionState.getStartedAt(), taskExecutionState.getCurrentDateTime());
            if (activePeriod.compareTo(taskExecutionState.getOverrideThreshold()) > 0)
                return true;

            return false;
        }
    }
}
