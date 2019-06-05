package com.siiconcatel.taskling.core.blocks.objectblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.contexts.ObjectBlockContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ObjectBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionErrorRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.retries.RetryService;

import java.util.Optional;
import java.util.function.Consumer;

public class ObjectBlockContextImpl<T> implements ObjectBlockContext {
    private final ObjectBlockRepository objectBlockRepository;
    private final TaskExecutionRepository taskExecutionRepository;
    private final String applicationName;
    private final String taskName;
    private final String taskExecutionId;

    public ObjectBlockContextImpl(ObjectBlockRepository objectBlockRepository,
                              TaskExecutionRepository taskExecutionRepository,
                              String applicationName,
                              String taskName,
                              String taskExecutionId,
                              ObjectBlock block,
                              String blockExecutionId,
                              String forcedBlockQueueId)
    {
        this.objectBlockRepository = objectBlockRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.block = block;
        this.blockExecutionId = blockExecutionId;
        this.forcedBlockQueueId = forcedBlockQueueId;
        this.applicationName = applicationName;
        this.taskName = taskName;
        this.taskExecutionId = taskExecutionId;
    }

    private ObjectBlock<T> block;
    private String blockExecutionId;
    private String forcedBlockQueueId;

    public ObjectBlock<T> getBlock() {
        return block;
    }

    public String getBlockExecutionId() {
        return blockExecutionId;
    }

    @Override
    public String getForcedBlockQueueId() {
        return forcedBlockQueueId;
    }

    public void start()
    {
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(new TaskId(applicationName, taskName),
                taskExecutionId,
                BlockType.Object,
                blockExecutionId,
                BlockExecutionStatus.Started);

        Consumer<BlockExecutionChangeStatusRequest> actionRequest = rq -> objectBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(actionRequest, request);
    }

    public void complete()
    {
        complete(-1);
    }

    public void complete(int itemsProcessed)
    {
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                BlockType.Object,
                blockExecutionId,
                BlockExecutionStatus.Completed);
        request.setItemsProcessed(itemsProcessed);

        Consumer<BlockExecutionChangeStatusRequest> actionRequest = rq -> objectBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(actionRequest, request);
    }

    public void failed()
    {
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                BlockType.Object,
                blockExecutionId,
                BlockExecutionStatus.Failed);

        Consumer<BlockExecutionChangeStatusRequest> actionRequest = rq -> objectBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(actionRequest, request);
    }

    public void failed(String message)
    {
        failed();

        String errorMessage = errorMessage = String.format("BlockId {0} Error: {1}",
                block.getObjectBlockId(),
                message);

        TaskExecutionErrorRequest errorRequest = new TaskExecutionErrorRequest(
            new TaskId(applicationName, taskName),
            taskExecutionId,
            errorMessage,
            false);

        taskExecutionRepository.error(errorRequest);
    }
}
