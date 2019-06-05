package com.siiconcatel.taskling.core.blocks.listblocks;

import com.siiconcatel.taskling.core.contexts.ListBlockContext;

import java.util.ArrayList;
import java.util.List;

public class ListBlockResponse<T> {
    List<ListBlockContext<T>> blockContexts;
    boolean allItemsIncluded;
    List<T> excludedItems;

    public ListBlockResponse(List<ListBlockContext<T>> blockContexts) {
        this.blockContexts = blockContexts;
        this.allItemsIncluded = true;
        this.excludedItems = new ArrayList<>();
    }

    public ListBlockResponse(List<ListBlockContext<T>> blockContexts, List<T> excludedItems) {
        this.blockContexts = blockContexts;
        this.allItemsIncluded = false;
        this.excludedItems = excludedItems;
    }

    public List<ListBlockContext<T>> getBlockContexts() {
        return blockContexts;
    }

    public boolean allItemsIncluded() {
        return allItemsIncluded;
    }

    public List<T> getExcludedItems() {
        return excludedItems;
    }
}
