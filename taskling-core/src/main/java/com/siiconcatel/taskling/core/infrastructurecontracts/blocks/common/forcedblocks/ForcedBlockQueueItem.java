package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;

public class ForcedBlockQueueItem {
    private BlockType blockType;
    private int forcedBlockQueueId;

    public ForcedBlockQueueItem(BlockType blockType, int forcedBlockQueueId) {
        this.blockType = blockType;
        this.forcedBlockQueueId = forcedBlockQueueId;
    }

    public BlockType getBlockType() {
        return blockType;
    }

    public void setBlockType(BlockType blockType) {
        this.blockType = blockType;
    }

    public int getForcedBlockQueueId() {
        return forcedBlockQueueId;
    }

    public void setForcedBlockQueueId(int forcedBlockQueueId) {
        this.forcedBlockQueueId = forcedBlockQueueId;
    }
}
