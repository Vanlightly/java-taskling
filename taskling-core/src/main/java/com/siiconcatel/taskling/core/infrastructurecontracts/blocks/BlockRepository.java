package com.siiconcatel.taskling.core.infrastructurecontracts.blocks;

import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
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

import java.util.List;

public interface BlockRepository {
    List<ForcedRangeBlockQueueItem> getQueuedForcedRangeBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
    List<ForcedListBlockQueueItem> getQueuedForcedListBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
    List<ForcedObjectBlockQueueItem> getQueuedForcedObjectBlocks(QueuedForcedBlocksRequest queuedForcedBlocksRequest);
    void dequeueForcedBlocks(DequeueForcedBlocksRequest dequeueForcedBlocksRequest);

    List<RangeBlock> findFailedRangeBlocks(FindFailedBlocksRequest failedBlocksRequest);
    List<RangeBlock> findDeadRangeBlocks(FindDeadBlocksRequest deadBlocksRequest);
    List<RangeBlock> findRangeBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest);
    RangeBlockCreateResponse addRangeBlock(RangeBlockCreateRequest rangeBlockCreateRequest);
    String addRangeBlockExecution(BlockExecutionCreateRequest executionCreateRequest);

    List<ProtoListBlock> findFailedListBlocks(FindFailedBlocksRequest failedBlocksRequest);
    List<ProtoListBlock> findDeadListBlocks(FindDeadBlocksRequest deadBlocksRequest);
    List<ProtoListBlock> findListBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest);
    ListBlockCreateResponse addListBlock(ListBlockCreateRequest createRequest);
    String addListBlockExecution(BlockExecutionCreateRequest executionCreateRequest);

    List<ProtoObjectBlock> findObjectBlocksOfTask(FindBlocksOfTaskRequest blocksOfTaskRequest);
    List<ProtoObjectBlock> findFailedObjectBlocks(FindFailedBlocksRequest failedBlocksRequest);
    List<ProtoObjectBlock> findDeadObjectBlocks(FindDeadBlocksRequest deadBlocksRequest);
    String addObjectBlockExecution(BlockExecutionCreateRequest executionCreateRequest);
    ObjectBlockCreateResponse addObjectBlock(ObjectBlockCreateRequest createRequest);
}
