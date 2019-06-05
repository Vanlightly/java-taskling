package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;

public class ForcedRangeBlockQueueItem extends ForcedBlockQueueItem {
    private RangeBlock rangeBlock;

    public ForcedRangeBlockQueueItem(BlockType blockType, int forcedBlockQueueId) {
        super(blockType, forcedBlockQueueId);
    }

    public RangeBlock getRangeBlock() {
        return rangeBlock;
    }

    public void setRangeBlock(RangeBlock rangeBlock) {
        this.rangeBlock = rangeBlock;
    }
}
