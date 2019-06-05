package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;

public class ForcedListBlockQueueItem extends ForcedBlockQueueItem {
    private ProtoListBlock listBlock;

    public ForcedListBlockQueueItem(BlockType blockType, int forcedBlockQueueId) {
        super(blockType, forcedBlockQueueId);
    }

    public ProtoListBlock getListBlock() {
        return listBlock;
    }

    public void setListBlock(ProtoListBlock listBlock) {
        this.listBlock = listBlock;
    }
}
