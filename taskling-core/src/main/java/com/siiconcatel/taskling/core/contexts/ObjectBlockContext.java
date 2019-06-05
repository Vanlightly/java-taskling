package com.siiconcatel.taskling.core.contexts;

import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.contexts.BlockContext;

import java.util.Optional;

/**
 * A block context that stores an arbitrary object
 * @param <T> The object type
 */
public interface ObjectBlockContext<T> extends BlockContext {
    /**
     * Returns the inner ObjectBlock
     * @return ObjectBlock
     */
    ObjectBlock<T> getBlock();

    /**
     * If the context was created by adding its id to the force block queue, this id has a non zero value.
     * @return The id of forced block queue entry that triggered the creation of this context
     */
    String getForcedBlockQueueId();
}
