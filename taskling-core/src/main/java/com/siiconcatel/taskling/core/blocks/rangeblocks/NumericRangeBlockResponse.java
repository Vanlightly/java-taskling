package com.siiconcatel.taskling.core.blocks.rangeblocks;

import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.NumericRangeBlockContext;

import java.time.Instant;
import java.util.List;

public class NumericRangeBlockResponse {
    List<NumericRangeBlockContext> blockContexts;
    boolean rangeCovered;
    long includedRangeEnd;
    long excludedRangeEnd;

    public NumericRangeBlockResponse(List<NumericRangeBlockContext> blockContexts) {
        this.blockContexts = blockContexts;
    }

    public NumericRangeBlockResponse(List<NumericRangeBlockContext> blockContexts,
                                     long includedRangeEnd,
                                     long excludedRangeEnd) {
        this.blockContexts = blockContexts;
        this.rangeCovered = false;
        this.includedRangeEnd = includedRangeEnd;
        this.excludedRangeEnd = excludedRangeEnd;
    }

    public List<NumericRangeBlockContext> getBlockContexts() {
        return blockContexts;
    }

    public boolean isRangeCovered() {
        return rangeCovered;
    }

    public long getIncludedRangeEnd() {
        return includedRangeEnd;
    }

    public long getExcludedRangeEnd() {
        return excludedRangeEnd;
    }
}
