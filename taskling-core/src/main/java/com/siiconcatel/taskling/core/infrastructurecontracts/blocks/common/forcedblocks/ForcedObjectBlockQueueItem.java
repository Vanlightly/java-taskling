package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;

public class ForcedObjectBlockQueueItem extends ForcedBlockQueueItem {
    private ProtoObjectBlock objectBlock;

    public ForcedObjectBlockQueueItem(BlockType blockType, int forcedBlockQueueId) {
        super(blockType, forcedBlockQueueId);
    }

    public ProtoObjectBlock getObjectBlock() {
        return objectBlock;
    }

    public void setObjectBlock(ProtoObjectBlock objectBlock) {
        this.objectBlock = objectBlock;
    }
}
