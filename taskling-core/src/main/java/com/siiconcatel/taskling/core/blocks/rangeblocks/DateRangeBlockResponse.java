package com.siiconcatel.taskling.core.blocks.rangeblocks;

import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;

import java.time.Instant;
import java.util.List;

public class DateRangeBlockResponse {
    List<DateRangeBlockContext> blockContexts;
    boolean rangeCovered;
    Instant includedRangeEnd;
    Instant excludedRangeEnd;

    public DateRangeBlockResponse(List<DateRangeBlockContext> blockContexts) {
        this.blockContexts = blockContexts;
        this.rangeCovered = true;
    }

    public DateRangeBlockResponse(List<DateRangeBlockContext> blockContexts, Instant includedRangeEnd, Instant excludedRangeEnd) {
        this.blockContexts = blockContexts;
        this.rangeCovered = false;
        this.includedRangeEnd = includedRangeEnd;
        this.excludedRangeEnd = excludedRangeEnd;
    }

    public List<DateRangeBlockContext> getBlockContexts() {
        return blockContexts;
    }

    public boolean isRangeCovered() {
        return rangeCovered;
    }

    public Instant getIncludedRangeEnd() {
        return includedRangeEnd;
    }

    public Instant getExcludedRangeEnd() {
        return excludedRangeEnd;
    }
}
