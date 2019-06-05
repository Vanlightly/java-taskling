package com.siiconcatel.taskling.core.infrastructurecontracts.blocks;

import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;

public interface ObjectBlockRepository {
    void changeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
    ProtoObjectBlock getLastObjectBlock(LastBlockRequest lastRangeBlockRequest);
}
