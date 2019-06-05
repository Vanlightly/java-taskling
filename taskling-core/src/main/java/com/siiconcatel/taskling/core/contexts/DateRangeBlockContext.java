package com.siiconcatel.taskling.core.contexts;

import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlock;

import java.util.Optional;

/**
 * A block context that stores a date range
 */
public interface DateRangeBlockContext extends BlockContext {
    /**
     * Returns the inner date range block
     * @return DateRangeBlock
     */
    DateRangeBlock getDateRangeBlock();

    /**
     * If the context was created by adding its id to the force block queue, this id has a non zero value.
     * @return The id of forced block queue entry that triggered the creation of this context
     */
    Optional<String> getForcedBlockQueueId();

    /**
     * Changes the status of the block execution to completed
     * @param itemsProcessed The number of items processed
     */
    void complete(int itemsProcessed);
}
