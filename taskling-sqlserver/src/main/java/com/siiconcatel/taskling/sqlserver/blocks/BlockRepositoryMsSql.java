package com.siiconcatel.taskling.sqlserver.blocks;

import com.siiconcatel.taskling.core.ConnectionStore;
import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.TransientException;
import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.ItemStatus;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindBlocksOfTaskRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindDeadBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindFailedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.*;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ListBlockCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ListBlockCreateResponse;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ObjectBlockCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ObjectBlockCreateResponse;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.rangeblocks.RangeBlockCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.rangeblocks.RangeBlockCreateResponse;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.core.tasks.ReprocessOption;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.core.utils.TicksHelper;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.*;
import com.siiconcatel.taskling.sqlserver.blocks.serialization.LargeValueCompressor;
import com.siiconcatel.taskling.sqlserver.blocks.serialization.SerializedValueReader;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

public class BlockRepositoryMsSql extends DbOperationsService implements BlockRepository {


    private final TaskRepository taskRepository;
    private static final String UnexpectedBlockTypeMessage = "This block type was not expected. This can occur when changing the block type of an existing process or combining different block types in a single process - which is not supported";

    public BlockRepositoryMsSql(TaskRepository taskRepository)
    {
        this.taskRepository = taskRepository;
    }

    public List<ForcedRangeBlockQueueItem> getQueuedForcedRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        String query = "";
        switch (queuedForcedBlocksRequest.getBlockType())
        {
            case DateRange:
                return getForcedDateRangeBlocks(queuedForcedBlocksRequest);
            case NumericRange:
                return getForcedNumericRangeBlocks(queuedForcedBlocksRequest);
            default:
                throw new TasklingExecutionException("This range type is not supported");
        }
    }

    public List<ForcedListBlockQueueItem> getQueuedForcedListBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return getForcedListBlocks(queuedForcedBlocksRequest);
    }

    public List<ForcedObjectBlockQueueItem> getQueuedForcedObjectBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        return getForcedObjectBlocks(queuedForcedBlocksRequest);
    }

    public void dequeueForcedBlocks(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
    {
        updateForcedBlocks(dequeueForcedBlocksRequest);
    }

    public List<RangeBlock> findFailedRangeBlocks(FindFailedBlocksRequest failedBlocksRequest)
    {
        String query = "";
        switch (failedBlocksRequest.getBlockType())
        {
            case DateRange:
                query = QueriesFailedBlocks.getFindFailedDateRangeBlocksQuery(failedBlocksRequest.getBlockCountLimit());
                break;
            case NumericRange:
                query = QueriesFailedBlocks.GetFindFailedNumericRangeBlocksQuery(failedBlocksRequest.getBlockCountLimit());
                break;
            default:
                throw new TasklingExecutionException("This range type is not supported");
        }

        return findFailedDateRangeBlocks(failedBlocksRequest, query);
    }

    public List<RangeBlock> findDeadRangeBlocks(FindDeadBlocksRequest deadBlocksRequest)
    {
        String query = "";
        switch (deadBlocksRequest.getBlockType())
        {
            case DateRange:
                if (deadBlocksRequest.getTaskDeathMode() == TaskDeathMode.KeepAlive)
                    query = QueriesDeadBlocks.getFindDeadDateRangeBlocksWithKeepAliveQuery(deadBlocksRequest.getBlockCountLimit());
                else
                    query = QueriesDeadBlocks.getFindDeadDateRangeBlocksQuery(deadBlocksRequest.getBlockCountLimit());
                break;
            case NumericRange:
                if (deadBlocksRequest.getTaskDeathMode() == TaskDeathMode.KeepAlive)
                    query = QueriesDeadBlocks.getFindDeadNumericRangeBlocksWithKeepAliveQuery(deadBlocksRequest.getBlockCountLimit());
                else
                    query = QueriesDeadBlocks.getFindDeadNumericRangeBlocksQuery(deadBlocksRequest.getBlockCountLimit());
                break;
            default:
                throw new TasklingExecutionException("This range type is not supported");
        }

        return findDeadDateRangeBlocks(deadBlocksRequest, query);
    }

    public List<RangeBlock> findRangeBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        String query = "";
        switch (blocksOfTaskRequest.getBlockType())
        {
            case DateRange:
                query = QueriesBlocksOfTask.getFindDateRangeBlocksOfTaskQuery(blocksOfTaskRequest.getReprocessOption());
                break;
            case NumericRange:
                query = QueriesBlocksOfTask.getFindNumericRangeBlocksOfTaskQuery(blocksOfTaskRequest.getReprocessOption());
                break;
            default:
                throw new TasklingExecutionException("This range type is not supported");
        }

        return findRangeBlocksOfTask(blocksOfTaskRequest, query);
    }

    public RangeBlockCreateResponse addRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(rangeBlockCreateRequest.getTaskId());

        RangeBlockCreateResponse response = new RangeBlockCreateResponse();
        switch (rangeBlockCreateRequest.getBlockType())
        {
            case DateRange:
                response.setBlock(addDateRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.getTaskDefinitionId()));
                break;
            case NumericRange:
                response.setBlock(addNumericRangeRangeBlock(rangeBlockCreateRequest, taskDefinition.getTaskDefinitionId()));
                break;
            default:
                throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
        }

        return response;
    }

    public String addRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
    {
        return addBlockExecution(executionCreateRequest);
    }

    public List<ProtoListBlock> findFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest)
    {
        if (failedBlocksRequest.getBlockType() == BlockType.List)
        {
            String query = QueriesFailedBlocks.GetFindFailedListBlocksQuery(failedBlocksRequest.getBlockCountLimit());
            return findFailedListBlocks(failedBlocksRequest, query);
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    public List<ProtoListBlock> findDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest)
    {
        if (deadBlocksRequest.getBlockType() == BlockType.List)
        {
            String query = "";
            if (deadBlocksRequest.getTaskDeathMode() == TaskDeathMode.KeepAlive)
                query = QueriesDeadBlocks.getFindDeadListBlocksWithKeepAliveQuery(deadBlocksRequest.getBlockCountLimit());
            else
                query = QueriesDeadBlocks.getFindDeadListBlocksQuery(deadBlocksRequest.getBlockCountLimit());

            return findDeadListBlocks(deadBlocksRequest, query);
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    public List<ProtoListBlock> findListBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        if (blocksOfTaskRequest.getBlockType() == BlockType.List)
        {
            String query = QueriesBlocksOfTask.getFindListBlocksOfTaskQuery(blocksOfTaskRequest.getReprocessOption());
            return findListBlocksOfTask(blocksOfTaskRequest, query, blocksOfTaskRequest.getReprocessOption());
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    public ListBlockCreateResponse addListBlock(ListBlockCreateRequest createRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(createRequest.getTaskId());

        if (createRequest.getBlockType() == BlockType.List)
        {
            long blockId = addNewListBlock(createRequest.getTaskId(),
                    taskDefinition.getTaskDefinitionId(),
                    createRequest.getSerializedHeader(),
                    createRequest.getCompressionThreshold());

            addListBlockItems(blockId, createRequest);

            // we do not populate the items here, they are lazy loaded
            ListBlockCreateResponse response = new ListBlockCreateResponse(
                    new ProtoListBlock(String.valueOf(blockId),0, createRequest.getSerializedHeader()));

            return response;
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    public String addListBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
    {
        return addBlockExecution(executionCreateRequest);
    }

    public List<ProtoObjectBlock> findObjectBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest)
    {
        if (blocksOfTaskRequest.getBlockType() == BlockType.Object)
        {
            String query = QueriesBlocksOfTask.GetFindObjectBlocksOfTaskQuery(blocksOfTaskRequest.getReprocessOption());
            return findObjectBlocksOfTask(blocksOfTaskRequest, query, blocksOfTaskRequest.getReprocessOption());
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    public List<ProtoObjectBlock> findFailedObjectBlocks(FindFailedBlocksRequest failedBlocksRequest)
    {
        if (failedBlocksRequest.getBlockType() == BlockType.Object)
        {
            String query = QueriesFailedBlocks.GetFindFailedObjectBlocksQuery(failedBlocksRequest.getBlockCountLimit());
            return findFailedObjectBlocks(failedBlocksRequest, query);
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    public List<ProtoObjectBlock> findDeadObjectBlocks(FindDeadBlocksRequest deadBlocksRequest)
    {
        if (deadBlocksRequest.getBlockType() == BlockType.Object)
        {
            String query = "";
            if (deadBlocksRequest.getTaskDeathMode() == TaskDeathMode.KeepAlive)
                query = QueriesDeadBlocks.getFindDeadObjectBlocksWithKeepAliveQuery(deadBlocksRequest.getBlockCountLimit());
            else
                query = QueriesDeadBlocks.getFindDeadObjectBlocksQuery(deadBlocksRequest.getBlockCountLimit());

            return findDeadObjectBlocks(deadBlocksRequest, query);
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    public String addObjectBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
    {
        return addBlockExecution(executionCreateRequest);
    }

    public ObjectBlockCreateResponse addObjectBlock(ObjectBlockCreateRequest createRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(createRequest.getTaskId());

        ObjectBlockCreateResponse response = new ObjectBlockCreateResponse();
        if (createRequest.getBlockType() == BlockType.Object)
        {
            long blockId = addNewObjectBlock(createRequest.getTaskId(),
                    taskDefinition.getTaskDefinitionId(),
                    createRequest.getObjectData(),
                    createRequest.getCompressionThreshold());

            response.setBlock(new ProtoObjectBlock(String.valueOf(blockId),
                0,
                createRequest.getObjectData()));

            return response;
        }

        throw new TasklingExecutionException(UnexpectedBlockTypeMessage);
    }

    private List<RangeBlock> findFailedDateRangeBlocks(FindFailedBlocksRequest failedBlocksRequest, String query)
    {
        List<RangeBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(failedBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(failedBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setTimestamp("searchPeriodBegin", TimeHelper.toTimestamp(failedBlocksRequest.getSearchPeriodBegin()));
            p.setTimestamp("searchPeriodEnd", TimeHelper.toTimestamp(failedBlocksRequest.getSearchPeriodEnd()));
            p.setInt("attemptLimit", failedBlocksRequest.getRetryLimit() + 1); // RetryLimit + 1st attempt

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != failedBlocksRequest.getBlockType())
                    throw new TasklingExecutionException(UnexpectedBlockTypeMessage);

                String rangeBlockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");

                long rangeBegin;
                long rangeEnd;
                if (failedBlocksRequest.getBlockType() == BlockType.DateRange)
                {
                    rangeBegin = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(2)));
                    rangeEnd = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(3)));
                }
                else
                {
                    rangeBegin = rs.getLong("FromNumber");
                    rangeEnd = rs.getLong("ToNumber");
                }

                results.add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd, failedBlocksRequest.getBlockType()));
            }

        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failed finding failed blocks", e);
        }

        return results;
    }

    private List<RangeBlock> findDeadDateRangeBlocks(FindDeadBlocksRequest deadBlocksRequest, String query)
    {
        List<RangeBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(deadBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(deadBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setTimestamp("searchPeriodBegin", TimeHelper.toTimestamp(deadBlocksRequest.getSearchPeriodBegin()));
            p.setTimestamp("searchPeriodEnd", TimeHelper.toTimestamp(deadBlocksRequest.getSearchPeriodEnd()));
            p.setInt("attemptLimit", deadBlocksRequest.getRetryLimit() + 1); // RetryLimit + 1st attempt

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != deadBlocksRequest.getBlockType())
                    throw new TasklingExecutionException(UnexpectedBlockTypeMessage);

                String rangeBlockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");

                long rangeBegin;
                long rangeEnd;
                if (deadBlocksRequest.getBlockType() == BlockType.DateRange)
                {
                    rangeBegin = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(2)));
                    rangeEnd = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(3)));
                }
                else
                {
                    rangeBegin = rs.getLong("FromNumber");
                    rangeEnd = rs.getLong("ToNumber");
                }

                results.add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd, deadBlocksRequest.getBlockType()));
            }

        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failed finding dead blocks", e);
        }

        return results;
    }

    private List<RangeBlock> findRangeBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest, String query)
    {
        List<RangeBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(blocksOfTaskRequest.getTaskId());

        try (Connection connection = createNewConnection(blocksOfTaskRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setString("referenceValue", blocksOfTaskRequest.getReferenceValueOfTask());

//            if (reprocessOption == ReprocessOption.PendingOrFailed)
//            {
//                p.setInt("notStarted", BlockExecutionStatus.NotStarted.getNumVal());
//                p.setInt("started", BlockExecutionStatus.Started.getNumVal());
//                p.setInt("failed", BlockExecutionStatus.Failed.getNumVal());
//            }

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != blocksOfTaskRequest.getBlockType())
                    throw new TasklingExecutionException("The block with this reference value is of a different BlockType. BlockType resuested: "
                            + blocksOfTaskRequest.getBlockType() + " BlockType found: " + blockType);

                String rangeBlockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");

                long rangeBegin;
                long rangeEnd;
                if (blocksOfTaskRequest.getBlockType() == BlockType.DateRange)
                {
                    rangeBegin = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(2)));
                    rangeEnd = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(3)));
                }
                else
                {
                    rangeBegin = rs.getLong("FromNumber");
                    rangeEnd = rs.getLong("ToNumber");
                }

                results.add(new RangeBlock(rangeBlockId, attempt, rangeBegin, rangeEnd, blocksOfTaskRequest.getBlockType()));
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure finding range blocks", e);
        }

        return results;
    }

    private RangeBlock addDateRangeRangeBlock(RangeBlockCreateRequest dateRangeBlockCreateRequest, int taskDefinitionId)
    {
        try (Connection connection = createNewConnection(dateRangeBlockCreateRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesRangeBlock.InsertDateRangeBlock);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setInt("blockType", BlockType.DateRange.getNumVal());
            p.setTimestamp("fromDate", TimeHelper.toTimestamp(TicksHelper.getDateFromTicks(dateRangeBlockCreateRequest.getFrom())));
            p.setTimestamp("toDate", TimeHelper.toTimestamp(TicksHelper.getDateFromTicks(dateRangeBlockCreateRequest.getTo())));

            ResultSet rs = p.executeQuery();
            rs.next();
            long id = rs.getLong(1);

            return new RangeBlock(String.valueOf(id),
                    0,
                    dateRangeBlockCreateRequest.getFrom(),
                    dateRangeBlockCreateRequest.getTo(),
                    dateRangeBlockCreateRequest.getBlockType());
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure adding a new numeric range block", e);
        }
    }

    private RangeBlock addNumericRangeRangeBlock(RangeBlockCreateRequest numericRangeBlockCreateRequest, int taskDefinitionId)
    {
        try (Connection connection = createNewConnection(numericRangeBlockCreateRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesRangeBlock.InsertNumericRangeBlock);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setInt("blockType", BlockType.NumericRange.getNumVal());
            p.setLong("fromNumber", numericRangeBlockCreateRequest.getFrom());
            p.setLong("toNumber", numericRangeBlockCreateRequest.getTo());

            ResultSet rs = p.executeQuery();
            rs.next();
            long id = rs.getLong(1);

            return new RangeBlock(String.valueOf(id),
                    0,
                    numericRangeBlockCreateRequest.getFrom(),
                    numericRangeBlockCreateRequest.getTo(),
                    numericRangeBlockCreateRequest.getBlockType());
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure adding a new numeric range block", e);
        }
    }

    private List<ProtoListBlock> findFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest, String query)
    {
        List<ProtoListBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(failedBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(failedBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setTimestamp("searchPeriodBegin", TimeHelper.toTimestamp(failedBlocksRequest.getSearchPeriodBegin()));
            p.setTimestamp("searchPeriodEnd", TimeHelper.toTimestamp(failedBlocksRequest.getSearchPeriodEnd()));
            p.setInt("attemptLimit", failedBlocksRequest.getRetryLimit() + 1); // RetryLimit + 1st attempt

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != failedBlocksRequest.getBlockType())
                    throw new TasklingExecutionException(UnexpectedBlockTypeMessage);

                String blockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");
                String objectData = SerializedValueReader.readOptionalValue(rs, "ObjectData", "CompressedObjectData");

                ProtoListBlock listBlock = new ProtoListBlock(blockId,attempt + 1, objectData);

                results.add(listBlock);
            }

        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failed finding failed blocks", e);
        }

        return results;
    }

    private List<ProtoListBlock> findDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest, String query)
    {
        List<ProtoListBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(deadBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(deadBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setTimestamp("searchPeriodBegin", TimeHelper.toTimestamp(deadBlocksRequest.getSearchPeriodBegin()));
            p.setTimestamp("searchPeriodEnd", TimeHelper.toTimestamp(deadBlocksRequest.getSearchPeriodEnd()));
            p.setInt("attemptLimit", deadBlocksRequest.getRetryLimit() + 1); // RetryLimit + 1st attempt

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != deadBlocksRequest.getBlockType())
                    throw new TasklingExecutionException(UnexpectedBlockTypeMessage);

                String blockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");
                String objectData = SerializedValueReader.readOptionalValue(rs, "ObjectData", "CompressedObjectData");

                ProtoListBlock listBlock = new ProtoListBlock(blockId,attempt + 1, objectData);

                results.add(listBlock);
            }

        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failed finding dead blocks", e);
        }

        return results;
    }

    private List<ProtoListBlock> findListBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest, String query, ReprocessOption reprocessOption)
    {
        List<ProtoListBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(blocksOfTaskRequest.getTaskId());

        try (Connection connection = createNewConnection(blocksOfTaskRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setString("referenceValue", blocksOfTaskRequest.getReferenceValueOfTask());

            if (reprocessOption == ReprocessOption.PendingOrFailed)
            {
                p.setInt("notStarted", BlockExecutionStatus.NotStarted.getNumVal());
                p.setInt("started", BlockExecutionStatus.Started.getNumVal());
                p.setInt("failed", BlockExecutionStatus.Failed.getNumVal());
            }

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != blocksOfTaskRequest.getBlockType())
                    throw new TasklingExecutionException("The block with this reference value is of a different BlockType. BlockType resuested: "
                            + blocksOfTaskRequest.getBlockType() + " BlockType found: " + blockType);

                String blockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");
                String objectData = SerializedValueReader.readOptionalValue(rs, "ObjectData", "CompressedObjectData");

                ProtoListBlock listBlock = new ProtoListBlock(blockId,attempt + 1, objectData);

                results.add(listBlock);
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure finding object blocks", e);
        }

        return results;
    }

    private long addNewListBlock(TaskId taskId, int taskDefinitionId, String header, int compressionThreshold)
    {
        if (header == null)
            header = "";

        boolean isLargeTextValue = false;
        byte[] compressedData = null;
        if (header.length() > compressionThreshold)
        {
            isLargeTextValue = true;
            compressedData = LargeValueCompressor.zip(header);
        }

        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesListBlock.InsertListBlock);
            p.setInt("taskDefinitionId", taskDefinitionId);

            if (isLargeTextValue)
            {
                p.setString("objectData", null);
                p.setBytes("compressedObjectData", compressedData);
            }
            else
            {
                p.setString("objectData", header);
                p.setBytes("compressedObjectData", null);
            }

            p.setInt("blockType", BlockType.List.getNumVal());

            ResultSet rs = p.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure adding a new list block", e);
        }
    }

    private void addListBlockItems(long blockId, ListBlockCreateRequest createRequest)
    {
        try (Connection connection = createNewConnection(createRequest.getTaskId()))
        {
            SQLServerBulkListRecord bulkListRecord = generateDataTable(blockId,
                    createRequest.getSerializedValues(),
                    createRequest.getCompressionThreshold());
            bulkLoadInTransactionOperation(bulkListRecord, "Taskling.ListBlockItem", connection);
            connection.commit();
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure adding a new object block", e);
        }
    }

    private long addNewObjectBlock(TaskId taskId, int taskDefinitionId, String objectData, int compressionThreshold)
    {
        boolean isLargeTextValue = false;
        byte[] compressedData = null;
        if (objectData.length() > compressionThreshold)
        {
            isLargeTextValue = true;
            compressedData = LargeValueCompressor.zip(objectData);
        }

        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesObjectBlock.InsertObjectBlock);
            p.setInt("taskDefinitionId", taskDefinitionId);

            if (isLargeTextValue)
            {
                p.setString("objectData", null);
                p.setObject("compressedObjectData", compressedData);
            }
            else
            {
                p.setString("objectData", objectData);
                p.setObject("compressedObjectData", null);
            }

            p.setInt("blockType", BlockType.Object.getNumVal());

            ResultSet rs = p.executeQuery();
            rs.next();
            return rs.getLong(1);
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure adding a new object block", e);
        }
    }

    private List<ProtoObjectBlock> findFailedObjectBlocks(FindFailedBlocksRequest failedBlocksRequest, String query)
    {
        List<ProtoObjectBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(failedBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(failedBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setTimestamp("searchPeriodBegin", TimeHelper.toTimestamp(failedBlocksRequest.getSearchPeriodBegin()));
            p.setTimestamp("searchPeriodEnd", TimeHelper.toTimestamp(failedBlocksRequest.getSearchPeriodEnd()));
            p.setInt("attemptLimit", failedBlocksRequest.getRetryLimit() + 1); // RetryLimit + 1st attempt

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != failedBlocksRequest.getBlockType())
                    throw new TasklingExecutionException(UnexpectedBlockTypeMessage);

                String blockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");
                String objectData = SerializedValueReader.readValue(rs, "ObjectData", "CompressedObjectData");

                ProtoObjectBlock objectBlock = new ProtoObjectBlock(blockId,attempt + 1, objectData);

                results.add(objectBlock);
            }

        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failed finding failed blocks", e);
        }

        return results;
    }

    private List<ProtoObjectBlock> findDeadObjectBlocks(FindDeadBlocksRequest deadBlocksRequest, String query)
    {
        List<ProtoObjectBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(deadBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(deadBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setTimestamp("searchPeriodBegin", TimeHelper.toTimestamp(deadBlocksRequest.getSearchPeriodBegin()));
            p.setTimestamp("searchPeriodEnd", TimeHelper.toTimestamp(deadBlocksRequest.getSearchPeriodEnd()));
            p.setInt("attemptLimit", deadBlocksRequest.getRetryLimit() + 1); // RetryLimit + 1st attempt

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != deadBlocksRequest.getBlockType())
                    throw new TasklingExecutionException(UnexpectedBlockTypeMessage);

                String blockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");
                String objectData = SerializedValueReader.readValue(rs, "ObjectData", "CompressedObjectData");

                ProtoObjectBlock objectBlock = new ProtoObjectBlock(blockId,attempt + 1, objectData);

                results.add(objectBlock);
            }

        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failed finding dead blocks", e);
        }

        return results;
    }

    private List<ProtoObjectBlock> findObjectBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest,
                                                          String query,
                                                          ReprocessOption reprocessOption)
    {
        List<ProtoObjectBlock> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(blocksOfTaskRequest.getTaskId());

        try (Connection connection = createNewConnection(blocksOfTaskRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setString("referenceValue", blocksOfTaskRequest.getReferenceValueOfTask());

            if (reprocessOption == ReprocessOption.PendingOrFailed)
            {
                p.setInt("notStarted", BlockExecutionStatus.NotStarted.getNumVal());
                p.setInt("started", BlockExecutionStatus.Started.getNumVal());
                p.setInt("failed", BlockExecutionStatus.Failed.getNumVal());
            }

            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType != blocksOfTaskRequest.getBlockType())
                    throw new TasklingExecutionException("The block with this reference value is of a different BlockType. BlockType resuested: "
                            + blocksOfTaskRequest.getBlockType() + " BlockType found: " + blockType);

                String blockId = String.valueOf(rs.getInt("BlockId"));
                int attempt = rs.getInt("Attempt");
                String objectData = SerializedValueReader.readValue(rs, "ObjectData", "CompressedObjectData");

                ProtoObjectBlock objectBlock = new ProtoObjectBlock(blockId,attempt + 1, objectData);

                results.add(objectBlock);
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure finding object blocks", e);
        }

        return results;
    }

    private List<ForcedRangeBlockQueueItem> getForcedDateRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        String query = QueriesForcedBlockQueue.GetDateRangeBlocksQuery();
        return getForcedRangeBlocks(queuedForcedBlocksRequest, query);
    }

    private List<ForcedRangeBlockQueueItem> getForcedNumericRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        String query = QueriesForcedBlockQueue.GetNumericRangeBlocksQuery();
        return getForcedRangeBlocks(queuedForcedBlocksRequest, query);
    }

    private List<ForcedRangeBlockQueueItem> getForcedRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest, String query)
    {
        List<ForcedRangeBlockQueueItem> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(queuedForcedBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(queuedForcedBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setString("status", "Pending");
            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType == queuedForcedBlocksRequest.getBlockType())
                {
                    String blockId = String.valueOf(rs.getInt("BlockId"));
                    int attempt = rs.getInt("Attempt");
                    int forceBlockQueueId = rs.getInt(4);

                    long rangeBegin;
                    long rangeEnd;

                    RangeBlock rangeBlock = null;
                    if (queuedForcedBlocksRequest.getBlockType() == BlockType.DateRange)
                    {
                        rangeBegin = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(2)));
                        rangeEnd = TicksHelper.getTicksFromDate(TimeHelper.toInstant(rs.getTimestamp(3)));
                        rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd, queuedForcedBlocksRequest.getBlockType());
                        forceBlockQueueId = rs.getInt(6);
                    }
                    else if (queuedForcedBlocksRequest.getBlockType() == BlockType.NumericRange)
                    {
                        rangeBegin = rs.getLong("FromNumber");
                        rangeEnd = rs.getLong("ToNumber");
                        rangeBlock = new RangeBlock(blockId, attempt + 1, rangeBegin, rangeEnd, queuedForcedBlocksRequest.getBlockType());
                        forceBlockQueueId = rs.getInt(6);
                    }

                    ForcedRangeBlockQueueItem queueItem = new ForcedRangeBlockQueueItem(
                            queuedForcedBlocksRequest.getBlockType(),
                            forceBlockQueueId);
                    queueItem.setRangeBlock(rangeBlock);

                    results.add(queueItem);
                }
                else
                {
                    throw new TasklingExecutionException("The block type of the process does not match the block type of the queued item. This could occur if the block type of the process has been changed during a new development. Expected: " + queuedForcedBlocksRequest.getBlockType() + " but queued block is: " + blockType);
                }
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure getting forced blocks", e);
        }

        return results;
    }

    private List<ForcedListBlockQueueItem> getForcedListBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        List<ForcedListBlockQueueItem> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(queuedForcedBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(queuedForcedBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesForcedBlockQueue.GetListBlocksQuery());
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setString("status", "Pending");
            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType == queuedForcedBlocksRequest.getBlockType())
                {
                    String blockId = String.valueOf(rs.getInt("BlockId"));
                    int attempt = rs.getInt("Attempt");
                    int forceBlockQueueId = rs.getInt(4);
                    String objectData = SerializedValueReader.readOptionalValue(rs, "ObjectData", "CompressedObjectData");

                    ProtoListBlock listBlock = new ProtoListBlock(blockId, attempt + 1, objectData);
                    ForcedListBlockQueueItem queueItem = new ForcedListBlockQueueItem(queuedForcedBlocksRequest.getBlockType(), forceBlockQueueId);
                    queueItem.setListBlock(listBlock);

                    results.add(queueItem);
                }
                else
                {
                    throw new TasklingExecutionException("The block type of the process does not match the block type of the queued item. This could occur if the block type of the process has been changed during a new development. Expected: " + queuedForcedBlocksRequest.getBlockType() + " but queued block is: " + blockType);
                }
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure getting forced blocks", e);
        }

        return results;
    }

    private List<ForcedObjectBlockQueueItem> getForcedObjectBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest)
    {
        List<ForcedObjectBlockQueueItem> results = new ArrayList<>();
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(queuedForcedBlocksRequest.getTaskId());

        try (Connection connection = createNewConnection(queuedForcedBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesForcedBlockQueue.GetObjectBlocksQuery());
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            p.setString("status", "Pending");
            ResultSet rs = p.executeQuery();
            while(rs.next()) {
                BlockType blockType = BlockType.valueOf(rs.getInt("BlockType"));
                if (blockType == queuedForcedBlocksRequest.getBlockType())
                {
                    String blockId = String.valueOf(rs.getInt("BlockId"));
                    int attempt = rs.getInt("Attempt");
                    int forceBlockQueueId = rs.getInt("ForceBlockQueueId");
                    String objectData = SerializedValueReader.readValue(rs, "ObjectData", "CompressedObjectData");

                    ProtoObjectBlock objectBlock = new ProtoObjectBlock(blockId,attempt + 1, objectData);

                    ForcedObjectBlockQueueItem queueItem = new ForcedObjectBlockQueueItem(
                            queuedForcedBlocksRequest.getBlockType(),
                            forceBlockQueueId);
                    queueItem.setObjectBlock(objectBlock);

                    results.add(queueItem);
                }
                else
                {
                    throw new TasklingExecutionException("The block type of the process does not match the block type of the queued item. This could occur if the block type of the process has been changed during a new development. Expected: " + queuedForcedBlocksRequest.getBlockType() + " but queued block is: " + blockType);
                }
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure getting forced blocks", e);
        }

        return results;
    }

    private void updateForcedBlocks(DequeueForcedBlocksRequest dequeueForcedBlocksRequest)
    {
        int blockCount = dequeueForcedBlocksRequest.getForcedBlockQueueIds().size();

        try (Connection connection = createNewConnection(dequeueForcedBlocksRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesForcedBlockQueue.GetUpdateQuery(blockCount));
            for (int i = 0; i < blockCount; i++)
                p.setInt("p" + i, Integer.parseInt(dequeueForcedBlocksRequest.getForcedBlockQueueIds().get(i)));

            p.executeUpdate();
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure updating the forced blocks queue", e);
        }
    }


    private SQLServerBulkListRecord generateDataTable(long blockId, List<String> values, int compressionThreshold)
    {
        List<Object[]> rows = new ArrayList<>();

        for (String value : values)
        {
            Object[] dr = new Object[5];
            dr[0] = blockId;

            if (value.length() > compressionThreshold)
            {
                dr[1] = null;
                dr[2] = LargeValueCompressor.zip(value);
            }
            else
            {
                dr[1] = value;
                dr[2] = null;
            }

            dr[3] = ItemStatus.Pending.getNumVal();
            dr[4] = TimeHelper.toTimestamp(Instant.now());
            rows.add(dr);
        }

        SQLServerBulkListRecord dt = new SQLServerBulkListRecord(rows);
        dt.addColumnMetadata(1, "BlockId", Types.BIGINT, 0, 0);
        dt.addColumnMetadata(2, "Value", Types.NVARCHAR, -1, 0);
        dt.addColumnMetadata(3, "CompressedValue", Types.VARBINARY, -1, 0);
        dt.addColumnMetadata(4, "Status", Types.SMALLINT, 0, 0);
        dt.addColumnMetadata(5, "LastUpdated", Types.DATE, 0, 0);

        return dt;
    }

    private Instant ensureSqlSafeDateTime(Instant dateTime)
    {
        if (dateTime.get(ChronoField.YEAR) < 1900)
            return Instant.ofEpochSecond(0);

        return dateTime;
    }

    private String addBlockExecution(BlockExecutionCreateRequest executionCreateRequest)
    {
        try (Connection connection = createNewConnection(executionCreateRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesRangeBlock.InsertBlockExecution);
            p.setLong("taskExecutionId", Long.parseLong(executionCreateRequest.getTaskExecutionId()));
            p.setLong("blockId", Long.parseLong(executionCreateRequest.getBlockId()));
            p.setInt("attempt", executionCreateRequest.getAttempt());
            p.setInt("status", BlockExecutionStatus.NotStarted.getNumVal());
            ResultSet rs = p.executeQuery();
            rs.next();
            return String.valueOf(rs.getLong(1));
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure adding a new block execution", e);
        }
    }


}
