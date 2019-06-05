package com.siiconcatel.taskling.core.infrastructurecontracts.blocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class LastBlockRequest {
    public LastBlockRequest(TaskId taskId,
                            BlockType blockType)
    {
        this.taskId = taskId;
        this.blockType = blockType;
    }

    private TaskId taskId;
    private BlockType blockType;
    private LastBlockOrder lastBlockOrder;

    public TaskId getTaskId() {
        return taskId;
    }

    public void setTaskId(TaskId taskId) {
        this.taskId = taskId;
    }

    public BlockType getBlockType() {
        return blockType;
    }

    public void setBlockType(BlockType blockType) {
        this.blockType = blockType;
    }

    public LastBlockOrder getLastBlockOrder() {
        return lastBlockOrder;
    }

    public void setLastBlockOrder(LastBlockOrder lastBlockOrder) {
        this.lastBlockOrder = lastBlockOrder;
    }
}
