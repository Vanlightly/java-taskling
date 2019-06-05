package com.siiconcatel.taskling.core.blocks.factories;

import com.siiconcatel.taskling.core.blocks.requests.BlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindDeadBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.DequeueForcedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionCheckpointRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;

import java.time.Instant;
import java.util.List;

public class BlockFactoryBase {

    protected TaskExecutionRepository taskExecutionRepository;
    protected BlockRepository blockRepository;

    public BlockFactoryBase(TaskExecutionRepository taskExecutionRepository,
                            BlockRepository blockRepository) {
        this.taskExecutionRepository = taskExecutionRepository;
        this.blockRepository = blockRepository;
    }

    protected FindDeadBlocksRequest createDeadBlocksRequest(BlockRequest blockRequest, int blockCountLimit) {
        Instant utcNow = Instant.now();

        return new FindDeadBlocksRequest(new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                utcNow.minus(blockRequest.getDeadTaskDetectionRange()),
                utcNow.minusSeconds(60),
                blockCountLimit,
                blockRequest.getTaskDeathMode(),
                blockRequest.getDeadTaskRetryLimit());
    }

    protected void logEmptyBlockEvent(String taskExecutionId, String appName, String taskName)
    {
        TaskExecutionCheckpointRequest checkPointRequest = new TaskExecutionCheckpointRequest(
                new TaskId(appName, taskName),
                taskExecutionId,
                "No values for generate the block. Emtpy Block context returned.");

        taskExecutionRepository.checkpoint(checkPointRequest);
    }

    protected void dequeueForcedBlocks(BlockRequest blockRequest, List<String> forcedBlockQueueIds)
    {
        DequeueForcedBlocksRequest request = new DequeueForcedBlocksRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                forcedBlockQueueIds);

        blockRepository.dequeueForcedBlocks(request);
    }
}
