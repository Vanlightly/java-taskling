package com.siiconcatel.taskling.sqlserver.blocks;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.TransientException;
import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.RangeBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.core.utils.TicksHelper;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TransientErrorDetector;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RangeBlockRepositoryMsSql extends DbOperationsService implements RangeBlockRepository {

    private final TaskRepository taskRepository;

    public RangeBlockRepositoryMsSql(TaskRepository taskRepository)
    {
        this.taskRepository = taskRepository;
    }

    public void changeStatus(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        switch (changeStatusRequest.getBlockType())
        {
            case DateRange:
                changeStatusOfDateRangeExecution(changeStatusRequest);
                break;
            case NumericRange:
                changeStatusOfNumericRangeExecution(changeStatusRequest);
                break;
            default:
                throw new TasklingExecutionException("This range type is not supported");
        }
    }

    public RangeBlock getLastRangeBlock(LastBlockRequest lastRangeBlockRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(lastRangeBlockRequest.getTaskId());

        String query = "";
        if (lastRangeBlockRequest.getBlockType() == BlockType.DateRange)
            query = QueriesRangeBlock.GetLastDateRangeBlock(lastRangeBlockRequest.getLastBlockOrder());
        else if (lastRangeBlockRequest.getBlockType() == BlockType.NumericRange)
            query = QueriesRangeBlock.GetLastNumericRangeBlock(lastRangeBlockRequest.getLastBlockOrder());
        else
            throw new TasklingExecutionException("An invalid BlockType was supplied: " + lastRangeBlockRequest.getBlockType());

        try (Connection connection = createNewConnection(lastRangeBlockRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());

            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                String rangeBlockId = String.valueOf(rs.getInt("BlockId"));

                long rangeBegin;
                long rangeEnd;
                if (lastRangeBlockRequest.getBlockType() == BlockType.DateRange)
                {
                    rangeBegin = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp("FromDate")));
                    rangeEnd = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp("ToDate")));
                }
                else
                {
                    rangeBegin = rs.getLong("FromNumber");
                    rangeEnd = rs.getLong("ToNumber");
                }

                return new RangeBlock(rangeBlockId, 0, rangeBegin, rangeEnd, lastRangeBlockRequest.getBlockType());
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure getting the last range block", e);
        }

        return null;
    }


    private void changeStatusOfDateRangeExecution(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try (Connection connection = createNewConnection(changeStatusRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, getDateRangeUpdateQuery(changeStatusRequest.getBlockExecutionStatus()));
            p.setLong("blockExecutionId", Long.parseLong(changeStatusRequest.getBlockExecutionId()));
            p.setInt("blockExecutionStatus", changeStatusRequest.getBlockExecutionStatus().getNumVal());

            if (changeStatusRequest.getBlockExecutionStatus() == BlockExecutionStatus.Completed || changeStatusRequest.getBlockExecutionStatus() == BlockExecutionStatus.Failed)
                p.setInt("itemsCount", changeStatusRequest.getItemsProcessed());
            p.executeUpdate();
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure changing block execution status", e);
        }
    }

    private void changeStatusOfNumericRangeExecution(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try (Connection connection = createNewConnection(changeStatusRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, getNumericRangeUpdateQuery(changeStatusRequest.getBlockExecutionStatus()));
            p.setLong("blockExecutionId", Long.parseLong(changeStatusRequest.getBlockExecutionId()));
            p.setInt("blockExecutionStatus", changeStatusRequest.getBlockExecutionStatus().getNumVal());

            if (changeStatusRequest.getBlockExecutionStatus() == BlockExecutionStatus.Completed || changeStatusRequest.getBlockExecutionStatus() == BlockExecutionStatus.Failed)
                p.setInt("itemsCount", changeStatusRequest.getItemsProcessed());
            p.executeUpdate();
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure changing block execution status", e);
        }
    }

    private String getDateRangeUpdateQuery(BlockExecutionStatus executionStatus)
    {
        if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
            return QueriesBlockExecution.SetRangeBlockExecutionAsCompleted;

        return QueriesBlockExecution.SetBlockExecutionStatusToStarted;
    }

    private String getNumericRangeUpdateQuery(BlockExecutionStatus executionStatus)
    {
        if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
            return QueriesBlockExecution.SetRangeBlockExecutionAsCompleted;

        return QueriesBlockExecution.SetBlockExecutionStatusToStarted;
    }
}
