package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

public class BlockExecutionChangeStatusRequest extends BlockRequestBase {
    public BlockExecutionChangeStatusRequest(TaskId taskId,
                                             String taskExecutionId,
                                             BlockType blockType,
                                             String blockExecutionId,
                                             BlockExecutionStatus blockExecutionStatus)
    {
        super(taskId, taskExecutionId, blockType, blockExecutionId);
        this.blockExecutionStatus = blockExecutionStatus;
    }

    private BlockExecutionStatus blockExecutionStatus;
    private int itemsProcessed;

    public BlockExecutionStatus getBlockExecutionStatus() {
        return blockExecutionStatus;
    }

    public void setBlockExecutionStatus(BlockExecutionStatus blockExecutionStatus) {
        this.blockExecutionStatus = blockExecutionStatus;
    }

    public int getItemsProcessed() {
        return itemsProcessed;
    }

    public void setItemsProcessed(int itemsProcessed) {
        this.itemsProcessed = itemsProcessed;
    }
}
