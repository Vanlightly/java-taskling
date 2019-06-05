package com.siiconcatel.taskling.sqlserver.taskexecution;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.*;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.core.tasks.TaskExecutionStatus;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;
import com.siiconcatel.taskling.sqlserver.events.EventsRepository;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenRepository;
import com.siiconcatel.taskling.sqlserver.tokens.executions.TokenRequest;
import com.siiconcatel.taskling.sqlserver.tokens.executions.TokenResponse;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;

public class TaskExecutionRepositoryMsSql extends DbOperationsService implements TaskExecutionRepository {
    private final TaskRepository taskRepository;
    private final ExecutionTokenRepository executionTokenRepository;
    private final EventsRepository eventsRepository;

    public TaskExecutionRepositoryMsSql(TaskRepository taskRepository,
                                   ExecutionTokenRepository executionTokenRepository,
                                   EventsRepository eventsRepository)
    {
        this.taskRepository = taskRepository;
        this.executionTokenRepository = executionTokenRepository;
        this.eventsRepository = eventsRepository;
    }

    public TaskExecutionStartResponse start(TaskExecutionStartRequest startRequest)
    {
        validateStartRequest(startRequest);
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(startRequest.getTaskId());

        if (startRequest.getTaskDeathMode() == TaskDeathMode.KeepAlive)
            return startKeepAliveExecution(startRequest, taskDefinition.getTaskDefinitionId());

        if (startRequest.getTaskDeathMode() == TaskDeathMode.Override)
            return startOverrideExecution(startRequest, taskDefinition.getTaskDefinitionId());

        throw new TasklingExecutionException("Unsupported TaskDeathMode");
    }

    public TaskExecutionCompleteResponse complete(TaskExecutionCompleteRequest completeRequest)
    {
        setCompletedDateOnTaskExecution(completeRequest.getTaskId(), completeRequest.getTaskExecutionId());
        registerEvent(completeRequest.getTaskId(), completeRequest.getTaskExecutionId(), EventType.End, null);
        return returnExecutionToken(completeRequest);
    }

    public void checkpoint(TaskExecutionCheckpointRequest taskExecutionRequest)
    {
        registerEvent(taskExecutionRequest.getTaskId(), taskExecutionRequest.getTaskExecutionId(), EventType.CheckPoint, taskExecutionRequest.getMessage());
    }

    public void error(TaskExecutionErrorRequest taskExecutionErrorRequest)
    {
        if (taskExecutionErrorRequest.isTreatTaskAsFailed())
            setTaskExecutionAsFailed(taskExecutionErrorRequest.getTaskId(), taskExecutionErrorRequest.getTaskExecutionId());

        registerEvent(taskExecutionErrorRequest.getTaskId(), taskExecutionErrorRequest.getTaskExecutionId(), EventType.Error, taskExecutionErrorRequest.getError());
    }

    public void sendKeepAlive(SendKeepAliveRequest sendKeepAliveRequest)
    {
        try (Connection connection = createNewConnection(sendKeepAliveRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecution.KeepAliveQuery);
            p.setInt("taskExecutionId", Integer.parseInt(sendKeepAliveRequest.getTaskExecutionId()));
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to set task exection as failed", e);
        }
    }

    public TaskExecutionMetaResponse getLastExecutionMetas(TaskExecutionMetaRequest taskExecutionMetaRequest)
    {
        TaskExecutionMetaResponse response = new TaskExecutionMetaResponse();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(taskExecutionMetaRequest.getTaskId());

        try (Connection connection = createNewConnection(taskExecutionMetaRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecution.GetLastExecutionQuery);
            p.setInt("top", taskExecutionMetaRequest.getExecutionsToRetrieve());
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                TaskExecutionMetaItem executionMeta = new TaskExecutionMetaItem();
                executionMeta.setStartedAt(TimeHelper.toInstant(rs.getTimestamp("StartedAt")));

                Timestamp completedAt = rs.getTimestamp("CompletedAt");
                if (completedAt != null)
                {
                    executionMeta.setCompletedAt(TimeHelper.toInstant(completedAt));

                    boolean failed = rs.getBoolean("Failed");
                    boolean blocked = rs.getBoolean("Blocked");

                    if (failed)
                        executionMeta.setStatus(TaskExecutionStatus.Failed);
                    else if (blocked)
                        executionMeta.setStatus(TaskExecutionStatus.Blocked);
                    else
                        executionMeta.setStatus(TaskExecutionStatus.Completed);
                }
                else
                {
                    TaskDeathMode taskDeathMode = TaskDeathMode.valueOf(rs.getInt("TaskDeathMode"));
                    if (taskDeathMode == TaskDeathMode.KeepAlive)
                    {
                        Instant lastKeepAlive = TimeHelper.toInstant(rs.getTimestamp("LastKeepAlive"));
                        Duration keepAliveThreshold = Duration.between(LocalTime.MIDNIGHT, rs.getTime("KeepAliveDeathThreshold").toLocalTime());
                        Instant dbServerUtcNow = TimeHelper.toInstant(rs.getTimestamp("DbServerUtcNow"));

                        Duration timeSinceLastKeepAlive = Duration.between(lastKeepAlive, dbServerUtcNow);
                        if (timeSinceLastKeepAlive.compareTo(keepAliveThreshold) > 0)
                            executionMeta.setStatus(TaskExecutionStatus.Dead);
                        else
                            executionMeta.setStatus(TaskExecutionStatus.InProgress);
                    }
                }

                if (rs.getString("ExecutionHeader") != null)
                    executionMeta.setHeader(rs.getString("ExecutionHeader"));

                if (rs.getString("ReferenceValue") != null)
                    executionMeta.setReferenceValue(rs.getString("ReferenceValue"));

                response.getExecutions().add(executionMeta);
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to set task exection as failed", e);
        }

        return response;
    }



    private void validateStartRequest(TaskExecutionStartRequest startRequest)
    {
        if (startRequest.getTaskDeathMode() == TaskDeathMode.KeepAlive)
        {
            if (!startRequest.getKeepAliveInterval().isPresent())
                throw new TasklingExecutionException("KeepAliveInterval must be set when using KeepAlive mode");

            if (!startRequest.getKeepAliveDeathThreshold().isPresent())
                throw new TasklingExecutionException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
        }
        else if (startRequest.getTaskDeathMode() == TaskDeathMode.Override)
        {
            if (!startRequest.getOverrideThreshold().isPresent())
                throw new TasklingExecutionException("OverrideThreshold must be set when using Override mode");
        }
    }

    private TaskExecutionStartResponse startKeepAliveExecution(TaskExecutionStartRequest startRequest, int taskDefinitionId)
    {
        int taskExecutionId = createKeepAliveTaskExecution(startRequest.getTaskId(),
            taskDefinitionId,
            startRequest.getKeepAliveInterval().get(),
            startRequest.getKeepAliveDeathThreshold().get(),
            startRequest.getReferenceValue(),
            startRequest.getFailedTaskRetryLimit(),
            startRequest.getDeadTaskRetryLimit(),
            startRequest.getTasklingVersion(),
            startRequest.getTaskExecutionHeader());

        registerEvent(startRequest.getTaskId(), String.valueOf(taskExecutionId), EventType.Start, null);
        TaskExecutionStartResponse tokenResponse = tryGetExecutionToken(startRequest.getTaskId(), taskDefinitionId, taskExecutionId, startRequest.getConcurrencyLimit());
        if (tokenResponse.getGrantStatus() == GrantStatus.Denied)
        {
            setBlockedOnTaskExecution(startRequest.getTaskId(), String.valueOf(taskExecutionId));
            if(tokenResponse.getEx() == null)
                registerEvent(startRequest.getTaskId(), String.valueOf(taskExecutionId), EventType.Blocked, null);
            else
                registerEvent(startRequest.getTaskId(), String.valueOf(taskExecutionId), EventType.Blocked, tokenResponse.getEx().toString());
        }

        return tokenResponse;
    }

    private TaskExecutionStartResponse startOverrideExecution(TaskExecutionStartRequest startRequest, int taskDefinitionId)
    {
        int taskExecutionId = createOverrideTaskExecution(startRequest.getTaskId(),
                taskDefinitionId,
                startRequest.getOverrideThreshold().get(),
                startRequest.getReferenceValue(),
                startRequest.getFailedTaskRetryLimit(),
                startRequest.getDeadTaskRetryLimit(),
                startRequest.getTasklingVersion(),
                startRequest.getTaskExecutionHeader());

        registerEvent(startRequest.getTaskId(), String.valueOf(taskExecutionId), EventType.Start, null);

        TaskExecutionStartResponse tokenResponse = tryGetExecutionToken(startRequest.getTaskId(), taskDefinitionId, taskExecutionId, startRequest.getConcurrencyLimit());

        if (tokenResponse.getGrantStatus() == GrantStatus.Denied)
        {
            setBlockedOnTaskExecution(startRequest.getTaskId(), String.valueOf(taskExecutionId));

            if(tokenResponse.getEx() == null)
                registerEvent(startRequest.getTaskId(), String.valueOf(taskExecutionId), EventType.Blocked, null);
            else
                registerEvent(startRequest.getTaskId(), String.valueOf(taskExecutionId), EventType.Blocked, tokenResponse.getEx().toString());
        }

        return tokenResponse;
    }

    private TaskExecutionStartResponse tryGetExecutionToken(TaskId taskId, int taskDefinitionId, int taskExecutionId, int concurrencyLimit)
    {
        TokenRequest tokenRequest = new TokenRequest(taskId,
            taskDefinitionId,
            String.valueOf(taskExecutionId),
            concurrencyLimit);

        try
        {
            TokenResponse tokenResponse = executionTokenRepository.tryAcquireExecutionToken(tokenRequest);

            TaskExecutionStartResponse response = new TaskExecutionStartResponse();
            response.setExecutionTokenId(tokenResponse.getExecutionTokenId());
            response.setGrantStatus(tokenResponse.getGrantStatus());
            response.setStartedAt(tokenResponse.getStartedAt());
            response.setTaskExecutionId(String.valueOf(taskExecutionId));

            return response;
        }
        catch(Exception ex)
        {
            TaskExecutionStartResponse response = new TaskExecutionStartResponse();
            response.setStartedAt(Instant.now());
            response.setGrantStatus(GrantStatus.Denied);
            response.setExecutionTokenId("0");
            response.setTaskExecutionId(String.valueOf(taskExecutionId));
            response.setEx(ex);

            return response;
        }
    }

    private int createKeepAliveTaskExecution(TaskId taskId,
                                             int taskDefinitionId,
                                             Duration keepAliveInterval,
                                             Duration keepAliveDeathThreshold,
                                             String referenceValue,
                                             int failedTaskRetryLimit,
                                             int deadTaskRetryLimit,
                                             String tasklingVersion,
                                             String executionHeader)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecution.InsertKeepAliveTaskExecution);

            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setString("serverName", getMachineName());
            p.setInt("taskDeathMode", TaskDeathMode.KeepAlive.getNumVal());
            p.setInt("failedTaskRetryLimit", failedTaskRetryLimit);
            p.setInt("deadTaskRetryLimit", deadTaskRetryLimit);
            p.setString("tasklingVersion", tasklingVersion);
            p.setString("executionHeader", executionHeader);
            p.setString("referenceValue", referenceValue);

            LocalTime kaLocalTime = LocalTime.MIDNIGHT.plus(keepAliveInterval);
            java.sql.Time keepAliveIntervalSqlTime = java.sql.Time.valueOf(kaLocalTime);
            p.setObject("keepAliveInterval", keepAliveIntervalSqlTime);

            LocalTime kadtLocalTime = LocalTime.MIDNIGHT.plus(keepAliveDeathThreshold);
            java.sql.Time keepAliveDeathThresholdSqlTime = java.sql.Time.valueOf(kadtLocalTime);
            p.setObject("keepAliveDeathThreshold", keepAliveDeathThresholdSqlTime);

            ResultSet rs = p.executeQuery();
            rs.next();
            return rs.getInt(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create task exection", e);
        }
    }

    private int createOverrideTaskExecution(TaskId taskId, int taskDefinitionId,
                                            Duration overrideThreshold,
                                            String referenceValue,
                                            int failedTaskRetryLimit,
                                            int deadTaskRetryLimit,
                                            String tasklingVersion,
                                            String executionHeader)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecution.InsertOverrideTaskExecution);

            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setString("serverName", getMachineName());
            p.setInt("taskDeathMode", TaskDeathMode.Override.getNumVal());
            p.setInt("failedTaskRetryLimit", failedTaskRetryLimit);
            p.setInt("deadTaskRetryLimit", deadTaskRetryLimit);
            p.setString("tasklingVersion", tasklingVersion);
            p.setString("executionHeader", executionHeader);
            p.setString("referenceValue", referenceValue);

            LocalTime localTime = LocalTime.MIDNIGHT.plus(overrideThreshold);
            java.sql.Time overrideThresholdSqlTime = java.sql.Time.valueOf(localTime);
            p.setObject("overrideThreshold", overrideThresholdSqlTime);

            ResultSet rs = p.executeQuery();
            rs.next();
            return rs.getInt(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create task exection", e);
        }
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

    private TaskExecutionCompleteResponse returnExecutionToken(TaskExecutionCompleteRequest taskExecutionCompleteRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(taskExecutionCompleteRequest.getTaskId());

        TokenRequest tokenRequest = new TokenRequest(
                taskExecutionCompleteRequest.getTaskId(),
                taskDefinition.getTaskDefinitionId(),
                taskExecutionCompleteRequest.getTaskExecutionId());

        executionTokenRepository.returnExecutionToken(tokenRequest, taskExecutionCompleteRequest.getExecutionTokenId());

        TaskExecutionCompleteResponse response = new TaskExecutionCompleteResponse();
        response.setCompletedAt(Instant.now());
        return response;
    }

    private void setBlockedOnTaskExecution(TaskId taskId, String taskExecutionId)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecution.SetBlockedTaskExecutionQuery);
            p.setInt("taskExecutionId", Integer.parseInt(taskExecutionId));
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to set blocked on task exection", e);
        }
    }

    private void setCompletedDateOnTaskExecution(TaskId taskId, String taskExecutionId)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecution.SetCompletedDateOfTaskExecutionQuery);
            p.setInt("taskExecutionId", Integer.parseInt(taskExecutionId));
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to set completed date on task exection", e);
        }
    }

    private void setTaskExecutionAsFailed(TaskId taskId, String taskExecutionId)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecution.SetTaskExecutionAsFailedQuery);
            p.setInt("taskExecutionId", Integer.parseInt(taskExecutionId));
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to set task exection as failed", e);
        }
    }

    private void registerEvent(TaskId taskId, String taskExecutionId, EventType eventType, String message)
    {
        eventsRepository.logEvent(taskId, taskExecutionId, eventType, message);
    }
}
