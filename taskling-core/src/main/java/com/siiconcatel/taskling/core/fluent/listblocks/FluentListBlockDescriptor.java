package com.siiconcatel.taskling.core.fluent.listblocks;

import com.siiconcatel.taskling.core.blocks.listblocks.BatchSize;
import com.siiconcatel.taskling.core.fluent.OverrideConfigurationDescriptor;
import com.siiconcatel.taskling.core.fluent.ReprocessScopeDescriptor;

import java.util.List;

public interface FluentListBlockDescriptor<T> {
    /**
     * Generates a list of ListBlockContext that cover the provided items, with a maximum of
     * maxBlockSize items per block.
     * The ListBlockContexts will be configured to synchronously updates the backend data store
     * on every ListBlockItem status change
     * @param values The items to be split into blocks
     * @param maxBlockSize The maximum number of items per block
     * @return
     */
    OverrideConfigurationDescriptor withSingleUnitCommit(List<T> values, short maxBlockSize);

    /**
     * Generates a list of ListBlockContext that cover the provided items, with a maximum of
     * maxBlockSize items per block.
     * The ListBlockContexts will be configured to periodically update the backend data store
     * every batchSize ListBlockItem status changes
     * @param values The items to be split into blocks
     * @param maxBlockSize The maximum number of items per block
     * @param batchSize Update the backed data store with batchSize items at a time
     * @return
     */
    OverrideConfigurationDescriptor withPeriodicCommit(List<T> values, short maxBlockSize, BatchSize batchSize);

    /**
     * Generates a list of ListBlockContext that cover the provided items, with a maximum of
     * maxBlockSize items per block.
     * The ListBlockContexts will be configured to update the backend data store
     * with all ListBlockItem status changes on changing the block status to complete
     * @param values The items to be split into blocks
     * @param maxBlockSize The maximum number of items per block
     * @return
     */
    OverrideConfigurationDescriptor withBatchCommitAtEnd(List<T> values, short maxBlockSize);

    /**
     * Instructs the execution context to reprocess blocks in single unit commit mode...
     * @return
     */
    ReprocessScopeDescriptor reprocessWithSingleUnitCommit();

    /**
     * Instructs the execution context to reprocess blocks in periodic commit mode...
     * @return
     */
    ReprocessScopeDescriptor reprocessWithPeriodicCommit(BatchSize batchSize);

    /**
     * Instructs the execution context to reprocess blocks in batch commit at end mode...
     * @return
     */
    ReprocessScopeDescriptor reprocessWithBatchCommitAtEnd();
}
