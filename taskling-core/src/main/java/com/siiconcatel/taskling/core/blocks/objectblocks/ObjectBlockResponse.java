package com.siiconcatel.taskling.core.blocks.objectblocks;

import com.siiconcatel.taskling.core.contexts.ObjectBlockContext;

import java.util.List;

public class ObjectBlockResponse<T> {
    List<ObjectBlockContext<T>> blockContexts;
    boolean objectIncluded;

    public ObjectBlockResponse(List<ObjectBlockContext<T>> blockContexts, boolean objectIncluded) {
        this.blockContexts = blockContexts;
        this.objectIncluded = objectIncluded;
    }

    public List<ObjectBlockContext<T>> getBlockContexts() {
        return blockContexts;
    }

    public boolean isObjectIncluded() {
        return objectIncluded;
    }
}
