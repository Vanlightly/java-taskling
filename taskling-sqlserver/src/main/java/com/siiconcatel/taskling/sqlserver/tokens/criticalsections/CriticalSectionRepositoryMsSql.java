package com.siiconcatel.taskling.sqlserver.tokens.criticalsections;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.criticalsection.CriticalSectionException;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.*;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.GrantStatus;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.core.utils.StringUtils;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.tokens.CommonTokenRepository;
import com.siiconcatel.taskling.sqlserver.tokens.QueriesTokens;
import com.siiconcatel.taskling.sqlserver.tokens.TaskExecutionState;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CriticalSectionRepositoryMsSql extends DbOperationsService implements CriticalSectionRepository {
    private final TaskRepository taskRepository;
    private final CommonTokenRepository commonTokenRepository;

    public CriticalSectionRepositoryMsSql(TaskRepository taskRepository,
                                     CommonTokenRepository commonTokenRepository)
    {
        this.taskRepository = taskRepository;
        this.commonTokenRepository = commonTokenRepository;
    }

    public StartCriticalSectionResponse start(StartCriticalSectionRequest startRequest)
    {
        validateStartRequest(startRequest);
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(startRequest.getTaskId());
        boolean granted = tryAcquireCriticalSection(startRequest.getTaskId(),
                taskDefinition.getTaskDefinitionId(),
                startRequest.getTaskExecutionId(),
                startRequest.getCriticalSectionType());

        return new StartCriticalSectionResponse(granted ? GrantStatus.Granted : GrantStatus.Denied);
    }

    public CompleteCriticalSectionResponse complete(CompleteCriticalSectionRequest completeRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(completeRequest.getTaskId());
        return returnCriticalSectionToken(completeRequest.getTaskId(),
                taskDefinition.getTaskDefinitionId(),
                completeRequest.getTaskExecutionId(),
                completeRequest.getCriticalSectionType());
    }

    private void validateStartRequest(StartCriticalSectionRequest startRequest)
    {
        if (startRequest.getTaskDeathMode() == TaskDeathMode.KeepAlive)
        {
            if (startRequest.getKeepAliveDeathThreshold() == null)
                throw new TasklingExecutionException("KeepAliveDeathThreshold must be set when using KeepAlive mode");
        }
        else if (startRequest.getTaskDeathMode() == TaskDeathMode.Override)
        {
            if (startRequest.getOverrideThreshold() == null)
                throw new TasklingExecutionException("OverrideThreshold must be set when using Override mode");
        }
    }

    private CompleteCriticalSectionResponse returnCriticalSectionToken(TaskId taskId,
                                                                         int taskDefinitionId,
                                                                         String taskExecutionId,
                                                                         CriticalSectionType criticalSectionType)
    {
        CompleteCriticalSectionResponse response = new CompleteCriticalSectionResponse();

        String query = "";
        if (criticalSectionType == CriticalSectionType.User)
            query = QueriesTokens.ReturnUserCriticalSectionTokenQuery;
        else
            query = QueriesTokens.ReturnClientCriticalSectionTokenQuery;

        try (Connection connection = createNewConnection(taskId)) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.execute();
            connection.commit();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create task definition", e);
        }

        return response;
    }

    private boolean tryAcquireCriticalSection(TaskId taskId, int taskDefinitionId, String taskExecutionId, CriticalSectionType criticalSectionType)
    {
        boolean granted = false;

        try (Connection connection = createNewConnection(taskId)) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            acquireRowLock(taskDefinitionId, taskExecutionId, connection);
            CriticalSectionState csState = getCriticalSectionState(taskDefinitionId, criticalSectionType, connection);
            cleanseOfExpiredExecutions(csState, connection);

            if (csState.isGranted())
            {
                // if the critical section is still granted to another execution after cleansing
                // then we rejected the request. If the execution is not in the queue then we add it
                if (!csState.existsInQueue(taskExecutionId))
                    csState.addToQueue(taskExecutionId);

                granted = false;
            }
            else
            {
                if (!csState.getQueue().isEmpty())
                {
                    if (csState.getFirstExecutionIdInQueue().equals(taskExecutionId))
                    {
                        grantCriticalSection(csState, taskExecutionId);
                        csState.removeFirstInQueue();
                        granted = true;
                    }
                    else
                    {
                        // not next in queue so cannot be granted the critical section
                        granted = false;
                    }
                }
                else
                {
                    grantCriticalSection(csState, taskExecutionId);
                    granted = true;
                }
            }

            if (csState.hasBeenModified())
                updateCriticalSectionState(taskDefinitionId, csState, criticalSectionType, connection);

            connection.commit();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create task definition", e);
        }



        return granted;
    }

    private void acquireRowLock(int taskDefinitionId, String taskExecutionId, Connection connection) throws SQLException
    {
        commonTokenRepository.acquireRowLock(taskDefinitionId, taskExecutionId, connection);
    }

    private CriticalSectionState getCriticalSectionState(int taskDefinitionId, CriticalSectionType criticalSectionType, Connection connection) throws SQLException
    {
        String query = "";
        if (criticalSectionType == CriticalSectionType.User)
            query = QueriesTokens.GetUserCriticalSectionStateQuery;
        else
            query = QueriesTokens.GetClientCriticalSectionStateQuery;

        NamedParameterStatement p= new NamedParameterStatement(connection, query);
        p.setInt("taskDefinitionId", taskDefinitionId);

        ResultSet rs = p.executeQuery();

        if(rs.next()) {
            CriticalSectionState csState = new CriticalSectionState();
            csState.setGranted(rs.getInt(getCsStatusColumnName(criticalSectionType)) == 0);
            csState.setGrantedToExecution(rs.getString(getGrantedToColumnName(criticalSectionType)));
            csState.setQueue(rs.getString(getQueueColumnName(criticalSectionType)));
            csState.startTrackingModifications();

            return csState;
        }

        throw new CriticalSectionException("No Task exists with id " + taskDefinitionId);
    }

    private List<String> getActiveTaskExecutionIds(CriticalSectionState csState)
    {
        List<String> taskExecutionIds = new ArrayList<>();

        if (!hasEmptyGranteeValue(csState))
            taskExecutionIds.add(csState.getGrantedToExecution());

        if (csState.HasQueuedExecutions())
            taskExecutionIds.addAll(csState.getQueue().stream().map(x -> x.getTaskExecutionId()).collect(Collectors.toList()));

        return taskExecutionIds;
    }

    private void cleanseOfExpiredExecutions(CriticalSectionState csState, Connection connection) throws SQLException
    {
        List<CriticalSectionQueueItem> csQueue = csState.getQueue();
        List<String> activeExecutionIds = getActiveTaskExecutionIds(csState);
        if (!activeExecutionIds.isEmpty())
        {
            List<TaskExecutionState> taskExecutionStates = getTaskExecutionStates(activeExecutionIds, connection);

            cleanseCurrentGranteeIfExpired(csState, taskExecutionStates);
            cleanseQueueOfExpiredExecutions(csState, taskExecutionStates, csQueue);
        }
    }

    private void cleanseCurrentGranteeIfExpired(CriticalSectionState csState, List<TaskExecutionState> taskExecutionStates)
    {
        if (!hasEmptyGranteeValue(csState) && csState.isGranted())
        {
            TaskExecutionState csStateOfGranted = taskExecutionStates.stream()
                    .filter(x -> x.getTaskExecutionId().equals(csState.getGrantedToExecution()))
                    .findFirst()
                    .get();

            if (hasCriticalSectionExpired(csStateOfGranted))
            {
                csState.setGranted(false);
            }
        }
    }

    private void cleanseQueueOfExpiredExecutions(CriticalSectionState csState,
                                                 List<TaskExecutionState> taskExecutionStates,
                                                 List<CriticalSectionQueueItem> csQueue)
    {
        List<CriticalSectionQueueItem> validQueuedExecutions = new ArrayList<>();
        for(TaskExecutionState teState : taskExecutionStates) {
            if(!hasCriticalSectionExpired(teState)) {
                List<CriticalSectionQueueItem> matching = csQueue.stream()
                        .filter(x -> x.getTaskExecutionId().equals(teState.getTaskExecutionId()))
                        .collect(Collectors.toList());

                validQueuedExecutions.addAll(matching);
            }
        }

        if (validQueuedExecutions.size() != csQueue.size())
        {
            List<CriticalSectionQueueItem> updatedQueue = new ArrayList<>();
            int newQueueIndex = 1;

            List<CriticalSectionQueueItem> sorted = validQueuedExecutions.stream()
                    .sorted(Comparator.comparing(CriticalSectionQueueItem::getIndex))
                    .collect(Collectors.toList());

            for (CriticalSectionQueueItem validQueuedExecution : sorted) {
                updatedQueue.add(new CriticalSectionQueueItem(newQueueIndex, validQueuedExecution.getTaskExecutionId()));
                newQueueIndex++;
            }

            csState.updateQueue(updatedQueue);
        }
    }

    private boolean hasEmptyGranteeValue(CriticalSectionState csState)
    {
        return StringUtils.isNullOrEmpty(csState.getGrantedToExecution())
                || csState.getGrantedToExecution().equals("0");
    }

    private void grantCriticalSection(CriticalSectionState csState, String taskExecutionId)
    {
        csState.setGranted(true);
        csState.setGrantedToExecution(taskExecutionId);
    }

    private void updateCriticalSectionState(int taskDefinitionId,
                                            CriticalSectionState csState,
                                            CriticalSectionType criticalSectionType,
                                            Connection connection) throws SQLException
    {
        String query = "";
        if (criticalSectionType == CriticalSectionType.User)
            query = QueriesTokens.SetUserCriticalSectionStateQuery;
        else
            query = QueriesTokens.SetClientCriticalSectionStateQuery;

        NamedParameterStatement p= new NamedParameterStatement(connection, query);
        p.setInt("taskDefinitionId", taskDefinitionId);
        p.setInt("csStatus", csState.isGranted() ? 0 : 1);
        p.setInt("csTaskExecutionId", Integer.parseInt(csState.getGrantedToExecution()));
        p.setString("csQueue", csState.getQueueString());

        p.execute();
    }

    private List<TaskExecutionState> getTaskExecutionStates(List<String> taskExecutionIds, Connection connection) throws SQLException
    {
        return commonTokenRepository.getTaskExecutionStates(taskExecutionIds, connection);
    }

    private boolean hasCriticalSectionExpired(TaskExecutionState taskExecutionState)
    {
        return commonTokenRepository.hasExpired(taskExecutionState);
    }

    private String getCsStatusColumnName(CriticalSectionType criticalSectionType)
    {
        if (criticalSectionType == CriticalSectionType.User)
            return "UserCsStatus";

        return "ClientCsStatus";
    }

    private String getGrantedToColumnName(CriticalSectionType criticalSectionType)
    {
        if (criticalSectionType == CriticalSectionType.User)
            return "UserCsTaskExecutionId";

        return "ClientCsTaskExecutionId";
    }

    private String getQueueColumnName(CriticalSectionType criticalSectionType)
    {
        if (criticalSectionType == CriticalSectionType.User)
            return "UserCsQueue";

        return "ClientCsQueue";
    }
}
