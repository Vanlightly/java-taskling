package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

public class QueuedForcedBlocksRequest extends BlockRequestBase {
    public QueuedForcedBlocksRequest(TaskId taskId,
                                     String taskExecutionId,
                                     BlockType blockType)
    {
        super(taskId, taskExecutionId, blockType);
    }
}
