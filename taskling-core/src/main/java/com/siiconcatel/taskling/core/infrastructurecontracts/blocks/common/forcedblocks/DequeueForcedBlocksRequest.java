package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

import java.util.List;

public class DequeueForcedBlocksRequest extends BlockRequestBase {
    public DequeueForcedBlocksRequest(TaskId taskId,
                                      String taskExecutionId,
                                      BlockType blockType,
                                      List<String> forcedBlockQueueIds)

    {
        super(taskId, taskExecutionId, blockType);
        this.forcedBlockQueueIds = forcedBlockQueueIds;
    }

    private List<String> forcedBlockQueueIds;

    public List<String> getForcedBlockQueueIds() {
        return forcedBlockQueueIds;
    }

    public void setForcedBlockQueueIds(List<String> forcedBlockQueueIds) {
        this.forcedBlockQueueIds = forcedBlockQueueIds;
    }
}
