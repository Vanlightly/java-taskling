package com.siiconcatel.taskling.core.fluent.rangeblocks;

import com.siiconcatel.taskling.core.fluent.OverrideConfigurationDescriptor;
import com.siiconcatel.taskling.core.fluent.ReprocessScopeDescriptor;

public interface FluentNumericRangeBlockDescriptor {
    /**
     * Instructs the execution context to create a list of NumericRangeBlocks that cover
     * the specified range, with the maxBlockNumberRange size of each block
     * @param fromNumber The start number of the whole range to be covered
     * @param toNumber The end number of the whole range to be covered
     * @param maxBlockNumberRange The maximum size of each block
     * @return
     */
    OverrideConfigurationDescriptor withRange(long fromNumber, long toNumber, long maxBlockNumberRange);

    /**
     * Instructs the execution context to only generate contexts for previously processed blocks (that failed)
     * @return
     */
    OverrideConfigurationDescriptor onlyOldNumericBlocks();

    /**
     * Instructs the execution context to reprocess a numeric range...
     * @return
     */
    ReprocessScopeDescriptor reprocessNumericRange();
}
