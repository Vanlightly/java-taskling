package com.siiconcatel.taskling.core.blocks.rangeblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.NumericRangeBlockContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.RangeBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionErrorRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.retries.RetryService;

import java.util.Optional;
import java.util.function.Consumer;

public class RangeBlockContextImpl implements DateRangeBlockContext, NumericRangeBlockContext {
    private final RangeBlockRepository rangeBlockRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final String applicationName;
    private final String taskName;
    private final String taskExecutionId;
    private RangeBlock block;
    private String blockExecutionId;
    private String forcedBlockQueueId;

    public RangeBlockContextImpl(RangeBlockRepository rangeBlockRepository,
                                 TaskExecutionRepository taskExecutionRepository,
                                 String applicationName,
                                 String taskName,
                                 String taskExecutionId,
                                 RangeBlock rangeBlock,
                                 String blockExecutionId)
    {
        this.rangeBlockRepository = rangeBlockRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.block = rangeBlock;
        this.blockExecutionId = blockExecutionId;
        this.forcedBlockQueueId = "0";
        this.applicationName = applicationName;
        this.taskName = taskName;
        this.taskExecutionId = taskExecutionId;
    }

    public RangeBlockContextImpl(RangeBlockRepository rangeBlockRepository,
                                 TaskExecutionRepository taskExecutionRepository,
                                 String applicationName,
                                 String taskName,
                                 String taskExecutionId,
                                 RangeBlock rangeBlock,
                                 String blockExecutionId,
                                 String forcedBlockQueueId)
    {
        this.rangeBlockRepository = rangeBlockRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.block = rangeBlock;
        this.blockExecutionId = blockExecutionId;
        this.forcedBlockQueueId = forcedBlockQueueId;
        this.applicationName = applicationName;
        this.taskName = taskName;
        this.taskExecutionId = taskExecutionId;
    }

    public DateRangeBlock getDateRangeBlock()
    {
        return block;
    }

    public NumericRangeBlock getNumericRangeBlock()
    {
        return block;
    }

    @Override
    public Optional<String> getForcedBlockQueueId()
    {
        if(forcedBlockQueueId == null)
            return Optional.empty();

        return Optional.of(forcedBlockQueueId);
    }

    public void start()
    {
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(new TaskId(applicationName, taskName),
                taskExecutionId,
                block.getRangeType(),
                blockExecutionId,
                BlockExecutionStatus.Started);

        Consumer<BlockExecutionChangeStatusRequest> consumer = rq -> rangeBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(consumer, request);
    }

    public void complete()
    {
        complete(-1);
    }

    public void complete(int itemsProcessed)
    {
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(new TaskId(applicationName, taskName),
                taskExecutionId,
                block.getRangeType(),
                blockExecutionId,
                BlockExecutionStatus.Completed);
        request.setItemsProcessed(itemsProcessed);

        Consumer<BlockExecutionChangeStatusRequest> consumer = rq -> rangeBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(consumer, request);
    }

    public void failed()
    {
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(new TaskId(applicationName, taskName),
                taskExecutionId,
                block.getRangeType(),
                blockExecutionId,
                BlockExecutionStatus.Failed);

        Consumer<BlockExecutionChangeStatusRequest> consumer = rq -> rangeBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(consumer, request);
    }

    public void failed(String message)
    {
        failed();

        String errorMessage = "";
        if (block.getRangeType() == BlockType.DateRange)
        {
            errorMessage = String.format("BlockId {0} From: {1} To: {2} Error: {3}",
                    block.getRangeBlockId(),
                    block.getStartDate().toString(),
                    block.getEndDate().toString(),
                    message);
        }
        else
        {
            errorMessage = String.format("BlockId {0} From: {1} To: {2} Error: {3}",
                    block.getRangeBlockId(),
                    block.getStartNumber(),
                    block.getEndNumber(),
                    message);
        }


        TaskExecutionErrorRequest errorRequest = new TaskExecutionErrorRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                errorMessage,
                false);

        taskExecutionRepository.error(errorRequest);
    }
}
