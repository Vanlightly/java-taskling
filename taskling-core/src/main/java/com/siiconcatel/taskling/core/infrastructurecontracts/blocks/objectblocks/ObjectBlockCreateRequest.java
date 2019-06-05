package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

public class ObjectBlockCreateRequest extends BlockRequestBase {
    public ObjectBlockCreateRequest(TaskId taskId,
                                    String taskExecutionId,
                                    String objectData,
                                    int compressionThreshold)
    {
        super(taskId, taskExecutionId, BlockType.Object);
        this.objectData = objectData;
        this.compressionThreshold = compressionThreshold;
    }

    private String objectData;
    private int compressionThreshold;

    public String getObjectData() {
        return objectData;
    }

    public void setObjectData(String objectData) {
        this.objectData = objectData;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }
}
