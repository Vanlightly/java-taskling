package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;

public class ItemHeaderLastBlockRequest<T,H> extends LastBlockRequest {
    private Class<T> itemType;
    private Class<H> headerType;

    public ItemHeaderLastBlockRequest(TaskId taskId,
                                      BlockType blockType,
                                      Class<T> itemType,
                                      Class<H> headerType) {
        super(taskId, blockType);
        this.itemType = itemType;
        this.headerType = headerType;
    }

    public Class<T> getItemType() {
        return itemType;
    }

    public void setItemType(Class<T> itemType) {
        this.itemType = itemType;
    }

    public Class<H> getHeaderType() {
        return headerType;
    }

    public void setHeaderType(Class<H> headerType) {
        this.headerType = headerType;
    }
}
