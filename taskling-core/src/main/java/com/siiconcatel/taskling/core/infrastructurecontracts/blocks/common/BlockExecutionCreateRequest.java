package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

public class BlockExecutionCreateRequest extends BlockRequestBase {
    public BlockExecutionCreateRequest(TaskId taskId,
                                       String taskExecutionId,
                                       BlockType blockType,
                                       String blockId,
                                       int attempt)
    {
        super(taskId, taskExecutionId, blockType);
        this.blockId = blockId;
        this.attempt = attempt;
    }

    private String blockId;
    private int attempt;

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }
}
