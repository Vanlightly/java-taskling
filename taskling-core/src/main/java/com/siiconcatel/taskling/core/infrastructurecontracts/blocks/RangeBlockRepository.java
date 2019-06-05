package com.siiconcatel.taskling.core.infrastructurecontracts.blocks;

import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;

public interface RangeBlockRepository {
    void changeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
    RangeBlock getLastRangeBlock(LastBlockRequest lastRangeBlockRequest);
}
