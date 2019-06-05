package com.siiconcatel.taskling.core.fluent.rangeblocks;

import com.siiconcatel.taskling.core.fluent.OverrideConfigurationDescriptor;
import com.siiconcatel.taskling.core.fluent.ReprocessScopeDescriptor;

import java.time.Duration;
import java.time.Instant;

public interface FluentDateRangeBlockDescriptor {
    /**
     * Instructs the execution context to create a list of DateRangeBlocks that cover
     * the specified range, with the maxBlockRange duration of each block
     * @param fromDate The start date of the whole range to be covered
     * @param toDate The end date of the whole range to be covered
     * @param maxBlockRange The maximum size of each block
     * @return
     */
    OverrideConfigurationDescriptor withRange(Instant fromDate, Instant toDate, Duration maxBlockRange);

    /**
     * Instructs the execution context to only generate contexts for previously processed blocks (that failed)
     * @return
     */
    OverrideConfigurationDescriptor onlyOldDateBlocks();

    /**
     * Instructs the execution context to reprocess a date range
     * @return
     */
    ReprocessScopeDescriptor reprocessDateRange();
}
