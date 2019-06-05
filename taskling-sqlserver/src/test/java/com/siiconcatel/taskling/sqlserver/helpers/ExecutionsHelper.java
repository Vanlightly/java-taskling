package com.siiconcatel.taskling.sqlserver.helpers;

import com.siiconcatel.taskling.core.ClientConnectionSettings;
import com.siiconcatel.taskling.core.ConnectionStore;
import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.core.utils.StringUtils;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenList;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenRepository;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenStatus;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ExecutionsHelper {
    public ExecutionsHelper()
    {
        TaskRepositoryMsSql.clearCache();
        ConnectionStore.getInstance().setConnection(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), new ClientConnectionSettings(TestConstants.TestConnectionString, TestConstants.QueryTimeout));
    }

    public void addConnection(TaskId taskId)
    {
        ConnectionStore.getInstance().setConnection(taskId, new ClientConnectionSettings(TestConstants.TestConnectionString, TestConstants.QueryTimeout));
    }

    private static final String InsertExecutionTokenQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [ExecutionTokens] = :executionTokens\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    private static final String GetExecutionTokensQuery =
            "SELECT ExecutionTokens\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "AND TaskName = :taskName";


    private static final String DeleteExecutionTokenQuery =
            "DELETE TEV FROM [Taskling].[TaskExecutionEvent] TEV\n" +
            "JOIN [Taskling].[TaskExecution] TE ON TEV.TaskExecutionId = TE.TaskExecutionId\n" +
            "JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE T.ApplicationName = :applicationName;\n" +
            "\n" +
            "DELETE TE FROM [Taskling].[TaskExecution] TE\n" +
            "JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE T.ApplicationName = :applicationName;\n" +
            "\n" +
            "DELETE FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName;\n" +
            "\n" +
            "DELETE FROM [Taskling].[ForceBlockQueue];";


    private static final String InsertTaskQuery =
            "INSERT INTO [Taskling].[TaskDefinition]([ApplicationName],[TaskName],[UserCsStatus],[ClientCsStatus])\n" +
            "VALUES(:applicationName,:taskName, 1, 1);\n" +
            "\n" +
            "SELECT [ApplicationName]\n" +
            "         ,[TaskName]\n" +
            "         ,[TaskDefinitionId]\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "AND TaskName = :taskName";

    private static final String SetKeepAliveQuery =
            "UPDATE TE\n" +
            "SET [LastKeepAlive] = :keepAliveDateTime\n" +
            "FROM [Taskling].[TaskExecution] TE\n" +
            "WHERE [TaskExecutionId] = :taskExecutionId";

    private static final String GetLastKeepAliveQuery =
            "SELECT MAX(LastKeepAlive)\n" +
            "FROM [Taskling].[TaskExecution]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId";


    private static final String InsertKeepAliveTaskExecutionQuery =
            "INSERT INTO [Taskling].[TaskExecution]\n" +
            "        ([TaskDefinitionId]\n" +
            "         ,[StartedAt]\n" +
            "         ,[LastKeepAlive]\n" +
            "         ,[ServerName]\n" +
            "         ,[TaskDeathMode]\n" +
            "         ,[KeepAliveInterval]\n" +
            "         ,[KeepAliveDeathThreshold]\n" +
            "         ,[FailedTaskRetryLimit]\n" +
            "         ,[DeadTaskRetryLimit]\n" +
            "         ,[Failed]\n" +
            "         ,[Blocked]\n" +
            "         ,[TasklingVersion])\n" +
            "VALUES\n" +
            "         (:taskDefinitionId\n" +
            "         ,:startedAt\n" +
            "         ,:completedAt\n" +
            "         ,:serverName\n" +
            "         ,:taskDeathMode\n" +
            "         ,:keepAliveInterval\n" +
            "         ,:keepAliveDeathThreshold\n" +
            "         ,:failedTaskRetryLimit\n" +
            "         ,:deadTaskRetryLimit\n" +
            "         ,0\n" +
            "         ,0\n" +
            "         ,'N/A');\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS INT)";


    private static final String InsertOverrideTaskExecutionQuery =
            "INSERT INTO [Taskling].[TaskExecution]\n" +
            "         ([TaskDefinitionId]\n" +
            "         ,[StartedAt]\n" +
            "         ,[LastKeepAlive]\n" +
            "         ,[ServerName]\n" +
            "         ,[TaskDeathMode]\n" +
            "         ,[OverrideThreshold]\n" +
            "         ,[FailedTaskRetryLimit]\n" +
            "         ,[DeadTaskRetryLimit]\n" +
            "         ,[Failed]\n" +
            "         ,[Blocked]\n" +
            "         ,[TasklingVersion])\n" +
            "VALUES\n" +
            "         (:taskDefinitionId\n" +
            "         ,:startedAt\n" +
            "         ,:completedAt\n" +
            "         ,:serverName\n" +
            "         ,:taskDeathMode\n" +
            "         ,:overrideThreshold\n" +
            "         ,:failedTaskRetryLimit\n" +
            "         ,:deadTaskRetryLimit\n" +
            "         ,0\n" +
            "         ,0\n" +
            "         ,'N/A');\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS INT)";

    private static final String UpdateTaskExecutionStatusQuery =
            "UPDATE [TasklingDb].[Taskling].[TaskExecution]\n" +
            "SET [CompletedAt] = GETUTCDATE()\n" +
            "WHERE TaskExecutionId = :taskExecutionId";

    private static final String SetLastExecutionAsDeadQuery =
            "UPDATE [Taskling].[TaskExecution]\n" +
            "SET CompletedAt = null,\n" +
            "    LastKeepAlive = DATEADD(HOUR, -6, GETUTCDATE()),\n" +
            "    StartedAt = DATEADD(HOUR, -6, GETUTCDATE())\n" +
            "WHERE TaskDefinitionId = :taskDefinitionId\n" +
            "AND TaskExecutionId = (SELECT MAX(TaskExecutionId) FROM [Taskling].[TaskExecution])";

    private static final String GetLastEventQuery =
            "SELECT [EventType]\n" +
            "        ,[Message]\n" +
            "FROM [Taskling].[TaskExecutionEvent] TEE\n" +
            "JOIN Taskling.TaskExecution AS TE ON TEE.TaskExecutionId = TE.TaskExecutionId\n" +
            "WHERE TE.TaskDefinitionId = :taskDefinitionId\n" +
            "ORDER BY 1 DESC";

    private static final String GetLastTaskExecutionQuery =
            "SELECT *\n" +
            "FROM [Taskling].[TaskExecution] TE\n" +
            "WHERE TE.TaskDefinitionId = :taskDefinitionId\n" +
            "ORDER BY 1 DESC";

    private static final String InsertCriticalSectionTokenQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [UserCsStatus] = :status\n" +
            "     ,[UserCsTaskExecutionId] = :taskExecutionId\n" +
            "     ,[HoldLockTaskExecutionId] = :taskExecutionId\n" +
            "WHERE TaskDefinitionId = :taskDefinitionId";

    private static final String GetQueueCountQuery =
            "SELECT COUNT(*)\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE [UserCsQueue] LIKE '%' + :taskExecutionId + '%'";

    private static final String InsertIntoCriticalSectionQueueQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [UserCsQueue] = COALESCE([UserCsQueue],'') + '|' + :csQueue\n" +
            "WHERE TaskDefinitionId = :taskDefinitionId";

    private static final String GetCriticalSectionTokenStatusByTaskExecutionQuery =
            "SELECT T.[UserCsStatus]\n" +
            "FROM [Taskling].[TaskExecution] TE\n" +
            "JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE T.ApplicationName = :applicationName\n" +
            "AND T.TaskName = :taskName";

    public void deleteRecordsOfApplication(String applicationName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, DeleteExecutionTokenQuery);
            p.setString("applicationName", applicationName);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void setKeepAlive(String taskExecutionId)
    {
        setKeepAlive(taskExecutionId, Instant.now());
    }

    public void setKeepAlive(String taskExecutionId, Instant keepAliveDateTime)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, SetKeepAliveQuery);
            p.setString("taskExecutionId", taskExecutionId);
            p.setTimestamp("keepAliveDateTime", TimeHelper.toTimestamp(keepAliveDateTime));
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public Instant getLastKeepAlive(int taskDefinitionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetLastKeepAliveQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            ResultSet rs = p.executeQuery();
            rs.next();

            return TimeHelper.toInstant(rs.getTimestamp(1));
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public LastEvent getLastEvent(int taskDefinitionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetLastEventQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                return new LastEvent(
                        EventType.valueOf(rs.getInt(1)),
                        rs.getString(2));
            }
            else {
                return null;
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public int insertTask(String applicationName, String taskName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertTaskQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                return rs.getInt("TaskDefinitionId");
            }
            else {
                return -1;
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void insertUnlimitedExecutionToken(int taskDefinitionId)
    {
        List<Pair<ExecutionTokenStatus, String>> tokens = new ArrayList<>();
        tokens.add(new Pair<ExecutionTokenStatus, String>(ExecutionTokenStatus.Unlimited, "0"));
        insertExecutionToken(taskDefinitionId, tokens);
    }

    public void insertUnavailableExecutionToken(int taskDefinitionId)
    {
        List<Pair<ExecutionTokenStatus, String>> tokens = new ArrayList<>();
        tokens.add(new Pair<ExecutionTokenStatus, String>(ExecutionTokenStatus.Unavailable, "0"));

        insertExecutionToken(taskDefinitionId, tokens);
    }

    public void insertAvailableExecutionToken(int taskDefinitionId, int count)
    {
        List<Pair<ExecutionTokenStatus, String>> list = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            list.add(new Pair<ExecutionTokenStatus, String>(ExecutionTokenStatus.Available, "0"));
        }

        insertExecutionToken(taskDefinitionId, list);
    }

    public void insertExecutionToken(int taskDefinitionId,
                                     List<Pair<ExecutionTokenStatus, String>> tokens)
    {
        String tokenString = generateTokensString(tokens);
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertExecutionTokenQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setString("executionTokens", tokenString);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    private String generateTokensString(List<Pair<ExecutionTokenStatus, String>> tokens)
    {
        StringBuilder sb = new StringBuilder();
        int counter = 0;
        for (Pair<ExecutionTokenStatus, String> token : tokens)
        {
            if (counter > 0)
                sb.append("|");

            sb.append("I:");
            sb.append(UUID.randomUUID().toString());
            sb.append(",S:");
            sb.append(String.valueOf(token.Item1.getNumVal()));
            sb.append(",G:");
            sb.append(token.Item2);

            counter++;
        }

        return sb.toString();
    }

    public ExecutionTokenList getExecutionTokens(String applicationName, String taskName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetExecutionTokensQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            ResultSet rs = p.executeQuery();
            rs.next();

            String tokenString = rs.getString(1);
            return ExecutionTokenRepositoryMsSql.parseTokensString(tokenString);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public ExecutionTokenStatus getExecutionTokenStatus(String applicationName, String taskName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetExecutionTokensQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            ResultSet rs = p.executeQuery();
            rs.next();
            String result = rs.getString(1);

            if (StringUtils.isNullOrEmpty(result))
                return ExecutionTokenStatus.Available;

            int pos = result.indexOf("S:") + 2;
            return ExecutionTokenStatus.valueOf(Integer.parseInt(result.substring(pos, pos+1)));
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public String insertKeepAliveTaskExecution(int taskDefinitionId)
    {
        return insertKeepAliveTaskExecution(taskDefinitionId, Duration.ofSeconds(20), Duration.ofSeconds(60));
    }

    public String insertOverrideTaskExecution(int taskDefinitionId)
    {
        return insertOverrideTaskExecution(taskDefinitionId, Duration.ofSeconds(60));
    }

    public String insertKeepAliveTaskExecution(int taskDefinitionId, Duration keepAliveInterval, Duration keepAliveDeathThreshold)
    {
        return insertKeepAliveTaskExecution(taskDefinitionId, keepAliveInterval, keepAliveDeathThreshold, Instant.now(), Instant.now());
    }

    public String insertKeepAliveTaskExecution(int taskDefinitionId, Duration keepAliveInterval, Duration keepAliveDeathThreshold, Instant startedAt, Instant completedAt)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertKeepAliveTaskExecutionQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setString("serverName", getMachineName());
            p.setInt("taskDeathMode", TaskDeathMode.KeepAlive.getNumVal());
            p.setObject("keepAliveInterval", toSqlTime(keepAliveInterval));
            p.setObject("keepAliveDeathThreshold", toSqlTime(keepAliveDeathThreshold));
            p.setTimestamp("startedAt", TimeHelper.toTimestamp(startedAt));
            p.setInt("failedTaskRetryLimit", 3);
            p.setInt("deadTaskRetryLimit", 3);

            if (completedAt != null)
                p.setTimestamp("completedAt", TimeHelper.toTimestamp(completedAt));
            else
                p.setTimestamp("completedAt", null);

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getString(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public String insertOverrideTaskExecution(int taskDefinitionId, Duration overrideThreshold)
    {
        return insertOverrideTaskExecution(taskDefinitionId, overrideThreshold, Instant.now(), Instant.now());
    }

    public String insertOverrideTaskExecution(int taskDefinitionId, Duration overrideThreshold, Instant startedAt, Instant completedAt)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertOverrideTaskExecutionQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setString("serverName", getMachineName());
            p.setInt("taskDeathMode", TaskDeathMode.Override.getNumVal());
            p.setObject("overrideThreshold", toSqlTime(overrideThreshold));
            p.setTimestamp("startedAt", TimeHelper.toTimestamp(startedAt));
            p.setInt("failedTaskRetryLimit", 3);
            p.setInt("deadTaskRetryLimit", 3);

            if (completedAt != null)
                p.setTimestamp("completedAt", TimeHelper.toTimestamp(completedAt));
            else
                p.setTimestamp("completedAt", null);

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getString(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void setTaskExecutionAsCompleted(String taskExecutionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, UpdateTaskExecutionStatusQuery);
            p.setString("taskExecutionId", taskExecutionId);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void setLastExecutionAsDead(int taskDefinitionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, SetLastExecutionAsDeadQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public boolean getBlockedStatusOfLastExecution(int taskDefinitionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetLastTaskExecutionQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                return rs.getBoolean("Blocked");
            }
            else {
                return false;
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public String getLastExecutionVersion(int taskDefinitionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetLastTaskExecutionQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                return rs.getString("TasklingVersion");
            }
            else {
                return "";
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public String getLastExecutionHeader(int taskDefinitionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetLastTaskExecutionQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                return rs.getString("ExecutionHeader");
            }
            else {
                return "";
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void insertUnavailableCriticalSectionToken(int taskDefinitionId, String taskExecutionId)
    {
        insertCriticalSectionToken(taskDefinitionId, taskExecutionId, (byte)0);
    }

    public void insertAvailableCriticalSectionToken(int taskDefinitionId, String taskExecutionId)
    {
        insertCriticalSectionToken(taskDefinitionId, taskExecutionId, (byte)1);
    }

    private void insertCriticalSectionToken(int taskDefinitionId, String taskExecutionId, byte status)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertCriticalSectionTokenQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setInt("taskExecutionId", Integer.parseInt(taskExecutionId));
            p.setInt("status", status);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public int getQueueCount(String taskExecutionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetQueueCountQuery);
            p.setString("taskExecutionId", taskExecutionId);
            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getInt(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void insertIntoCriticalSectionQueue(int taskDefinitionId, int queueIndex, String taskExecutionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertIntoCriticalSectionQueueQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setString("csQueue", queueIndex + "," + taskExecutionId);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public byte getCriticalSectionTokenStatus(String applicationName, String taskName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetCriticalSectionTokenStatusByTaskExecutionQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getByte(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    protected Connection createNewConnection() throws SQLException
    {
        return DriverManager.getConnection(TestConstants.TestConnectionString);
    }

    private String getMachineName() {
        try
        {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            return addr.getHostName();
        }
        catch (UnknownHostException ex)
        {
            return "unknown";
        }
    }

    private java.sql.Time toSqlTime(Duration duration) {
        LocalTime localTime = LocalTime.MIDNIGHT.plus(duration);
        return java.sql.Time.valueOf(localTime);
    }
}
