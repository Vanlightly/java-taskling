package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.ListUpdateMode;

import java.util.List;

public class ItemOnlyListBlockRequest<T> extends ListBlockRequest {

    private Class<T> itemType;

    public ItemOnlyListBlockRequest(Class<T> itemType) {
        super();
        this.itemType = itemType;
    }

    public Class<T> getItemType() {
        return itemType;
    }

    public void setItemType(Class<T> itemType) {
        this.itemType = itemType;
    }
}
