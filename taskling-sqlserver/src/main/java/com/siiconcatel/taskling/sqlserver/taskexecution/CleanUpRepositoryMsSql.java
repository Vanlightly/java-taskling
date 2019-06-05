package com.siiconcatel.taskling.sqlserver.taskexecution;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.cleanup.CleanUpRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.cleanup.CleanUpRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CleanUpRepositoryMsSql extends DbOperationsService implements CleanUpRepository {
    private final TaskRepository taskRepository;

    public CleanUpRepositoryMsSql(TaskRepository taskRepository)
    {
        this.taskRepository = taskRepository;
    }

    public boolean cleanOldData(CleanUpRequest cleanUpRequest)
    {
        Instant lastCleaned = taskRepository.getLastTaskCleanUpTime(cleanUpRequest.getTaskId());
        Duration periodSinceLastClean = Duration.between(Instant.now(), lastCleaned);

        if (periodSinceLastClean.compareTo(cleanUpRequest.getTimeSinceLastCleaningThreashold()) > 0)
        {
            taskRepository.setLastCleaned(cleanUpRequest.getTaskId());
            TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(cleanUpRequest.getTaskId());
            cleanListItems(cleanUpRequest.getTaskId(), taskDefinition.getTaskDefinitionId(), cleanUpRequest.getListItemDateThreshold());
            cleanOldData(cleanUpRequest.getTaskId(), taskDefinition.getTaskDefinitionId(), cleanUpRequest.getGeneralDateThreshold());
            return true;
        }

        return false;
    }

    private void cleanListItems(TaskId taskId, int taskDefinitionId, Instant listItemDateThreshold) {
        try (Connection connection = createNewConnection(taskId)) {
            List<Long> blockIds = identifyOldBlocks(taskId, connection, taskDefinitionId, listItemDateThreshold);
            for (Long blockId : blockIds)
                deleteListItemsOfBlock(connection, blockId);
        } catch (SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create task definition", e);
        }
    }

    private List<Long> identifyOldBlocks(TaskId taskId, Connection connection, int taskDefinitionId, Instant listItemDateThreshold) throws SQLException
    {
        List<Long> blockIds = new ArrayList<>();
        NamedParameterStatement p= new NamedParameterStatement(connection, QueriesCleanUp.IdentifyOldBlocksQuery);
        p.setInt("taskDefinitionId", taskDefinitionId);
        p.setTimestamp("olderThanDate", TimeHelper.toTimestamp(listItemDateThreshold));
        ResultSet rs = p.executeQuery();

        while(rs.next()) {
            blockIds.add(rs.getLong(1));
        }

        return blockIds;
    }

    private void deleteListItemsOfBlock(Connection connection, long blockId) throws SQLException
    {
        NamedParameterStatement p= new NamedParameterStatement(connection, QueriesCleanUp.DeleteListItemsOfBlockQuery);
        p.setLong("blockId", blockId);
        p.execute();
    }

    private void cleanOldData(TaskId taskId, int taskDefinitionId, Instant generalDateThreshold)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesCleanUp.DeleteOldDataQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setTimestamp("olderThanDate", TimeHelper.toTimestamp(generalDateThreshold));
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create task definition", e);
        }
    }
}
