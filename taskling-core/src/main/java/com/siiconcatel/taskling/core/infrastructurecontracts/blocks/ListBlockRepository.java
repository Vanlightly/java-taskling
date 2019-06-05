package com.siiconcatel.taskling.core.infrastructurecontracts.blocks;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.BatchUpdateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlockItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.SingleUpdateRequest;

import java.util.List;

public interface ListBlockRepository {
    void changeStatus(BlockExecutionChangeStatusRequest changeStatusRequest);
    List<ProtoListBlockItem> getListBlockItems(TaskId taskId, String listBlockId);
    void updateListBlockItem(SingleUpdateRequest singeUpdateRequest);
    void batchUpdateListBlockItems(BatchUpdateRequest batchUpdateRequest);
    ProtoListBlock getLastListBlock(LastBlockRequest lastRangeBlockRequest);
}
