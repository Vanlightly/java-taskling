package com.siiconcatel.taskling.core.blocks.listblocks;

import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.contexts.ListBlockWithHeaderContext;

import java.util.ArrayList;
import java.util.List;

public class ListBlockWithHeaderResponse<T,H> {
    List<ListBlockWithHeaderContext<T,H>> blockContexts;
    boolean allItemsIncluded;
    List<T> excludedItems;

    public ListBlockWithHeaderResponse(List<ListBlockWithHeaderContext<T,H>> blockContexts) {
        this.blockContexts = blockContexts;
        this.allItemsIncluded = true;
        this.excludedItems = new ArrayList<>();
    }

    public ListBlockWithHeaderResponse(List<ListBlockWithHeaderContext<T,H>> blockContexts, List<T> excludedItems) {
        this.blockContexts = blockContexts;
        this.allItemsIncluded = false;
        this.excludedItems = excludedItems;
    }

    public List<ListBlockWithHeaderContext<T,H>> getBlockContexts() {
        return blockContexts;
    }

    public boolean allItemsIncluded() {
        return allItemsIncluded;
    }

    public List<T> getExcludedItems() {
        return excludedItems;
    }
}
