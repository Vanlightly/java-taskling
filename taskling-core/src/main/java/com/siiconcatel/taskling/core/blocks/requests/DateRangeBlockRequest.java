package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class DateRangeBlockRequest extends BlockRequest {
    public DateRangeBlockRequest()
    {
        super.setBlockType(BlockType.DateRange);
    }

    private Instant rangeBegin;
    private Instant rangeEnd;
    private Duration maxBlockRange;

    public Optional<Instant> getRangeBegin() {
        if(rangeBegin == null)
            return Optional.empty();
        return Optional.of(rangeBegin);
    }

    public void setRangeBegin(Instant rangeBegin) {

        this.rangeBegin = rangeBegin;
    }

    public Optional<Instant> getRangeEnd() {
        if(rangeEnd == null)
            return Optional.empty();
        return Optional.of(rangeEnd);
    }

    public void setRangeEnd(Instant rangeEnd) {
        this.rangeEnd = rangeEnd;
    }

    public Optional<Duration> getMaxBlockRange() {
        if(maxBlockRange == null)
            return Optional.empty();
        return Optional.of(maxBlockRange);
    }

    public void setMaxBlockRange(Duration maxBlockRange) {

        this.maxBlockRange = maxBlockRange;
    }
}
