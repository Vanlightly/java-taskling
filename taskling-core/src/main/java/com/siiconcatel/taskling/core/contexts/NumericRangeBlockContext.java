package com.siiconcatel.taskling.core.contexts;

import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlock;

import java.util.Optional;

/**
 * A block context that stores a numeric range
 */
public interface NumericRangeBlockContext extends BlockContext {
    /**
     * Returns the inner numeric block
     * @return NumericRangeBlock
     */
    NumericRangeBlock getNumericRangeBlock();

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
