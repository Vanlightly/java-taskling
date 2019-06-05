package com.siiconcatel.taskling.core.infrastructurecontracts.blocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class BlockRequestBase {
    public BlockRequestBase(TaskId taskId, String taskExecutionId, BlockType blockType)
    {
        this.taskId = taskId;
        this.taskExecutionId = taskExecutionId;
        this.blockType = blockType;
    }

    public BlockRequestBase(TaskId taskId, String taskExecutionId, BlockType blockType, String blockExecutionId)
    {
        this.taskId = taskId;
        this.taskExecutionId = taskExecutionId;
        this.blockExecutionId = blockExecutionId;
        this.blockType = blockType;
    }

    private TaskId taskId;
    private String taskExecutionId;
    private String blockExecutionId;
    private BlockType blockType;

    public TaskId getTaskId() {
        return taskId;
    }

    public void setTaskId(TaskId taskId) {
        this.taskId = taskId;
    }

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        this.taskExecutionId = taskExecutionId;
    }

    public String getBlockExecutionId() {
        return blockExecutionId;
    }

    public void setBlockExecutionId(String blockExecutionId) {
        this.blockExecutionId = blockExecutionId;
    }

    public BlockType getBlockType() {
        return blockType;
    }

    public void setBlockType(BlockType blockType) {
        this.blockType = blockType;
    }
}
