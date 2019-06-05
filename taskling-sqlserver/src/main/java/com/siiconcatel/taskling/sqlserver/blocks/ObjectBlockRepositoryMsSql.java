package com.siiconcatel.taskling.sqlserver.blocks;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.TransientException;
import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ObjectBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.ForcedObjectBlockQueueItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TransientErrorDetector;
import com.siiconcatel.taskling.sqlserver.blocks.serialization.SerializedValueReader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ObjectBlockRepositoryMsSql extends DbOperationsService implements ObjectBlockRepository
{
    private final TaskRepository taskRepository;

    public ObjectBlockRepositoryMsSql(TaskRepository taskRepository)
    {
        this.taskRepository = taskRepository;
    }

    public void changeStatus(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        changeStatusOfExecution(changeStatusRequest);
    }

    public ProtoObjectBlock getLastObjectBlock(LastBlockRequest lastObjectBlockRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(lastObjectBlockRequest.getTaskId());

        try (Connection connection = createNewConnection(lastObjectBlockRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesObjectBlock.GetLastObjectBlock);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType == lastObjectBlockRequest.getBlockType())
                {
                    String blockId = String.valueOf(rs.getInt("BlockId"));
                    String objectData = SerializedValueReader.readValue(rs, "ObjectData", "CompressedObjectData");

                    ProtoObjectBlock objectBlock = new ProtoObjectBlock(blockId,0, objectData);

                    return objectBlock;
                }
                else
                {
                    throw new TasklingExecutionException("The block type of the process does not match the block type of the queued item. This could occur if the block type of the process has been changed during a new development. Expected: "
                            + lastObjectBlockRequest.getBlockType() + " but queued block is: " + blockType);
                }
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure getting object block", e);
        }

        return null;
    }


    private void changeStatusOfExecution(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try (Connection connection = createNewConnection(changeStatusRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, getUpdateQuery(changeStatusRequest.getBlockExecutionStatus()));
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

    private String getUpdateQuery(BlockExecutionStatus executionStatus)
    {
        if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
            return QueriesBlockExecution.SetRangeBlockExecutionAsCompleted;

        return QueriesBlockExecution.SetBlockExecutionStatusToStarted;
    }
}
