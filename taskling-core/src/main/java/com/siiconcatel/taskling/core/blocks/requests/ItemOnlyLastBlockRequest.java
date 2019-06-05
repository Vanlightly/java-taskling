package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;

public class ItemOnlyLastBlockRequest<T> extends LastBlockRequest {
    private Class<T> itemType;

    public ItemOnlyLastBlockRequest(TaskId taskId,
                                    BlockType blockType,
                                    Class<T> itemType) {
        super(taskId, blockType);
        this.itemType = itemType;
    }

    public Class<T> getItemType() {
        return itemType;
    }

    public void setItemType(Class<T> itemType) {
        this.itemType = itemType;
    }
}
