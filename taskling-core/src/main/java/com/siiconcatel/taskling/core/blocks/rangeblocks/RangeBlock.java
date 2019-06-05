package com.siiconcatel.taskling.core.blocks.rangeblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.utils.TicksHelper;

import java.time.Instant;

public class RangeBlock implements DateRangeBlock, NumericRangeBlock {


    private String rangeBlockId;
    private int attempt;
    private long rangeBegin;
    private long rangeEnd;
    private BlockType rangeType;

    public RangeBlock(String rangeBlockId, int attempt, long rangeBegin, long rangeEnd, BlockType rangeType) {
        this.rangeBlockId = rangeBlockId;
        this.attempt = attempt;
        this.rangeBegin = rangeBegin;
        this.rangeEnd = rangeEnd;
        this.rangeType = rangeType;
    }

    @Override
    public String getRangeBlockId() {
        return rangeBlockId;
    }

    @Override
    public int getAttempt() {
        return attempt;
    }

    @Override
    public long getStartNumber() {
        return rangeBegin;
    }

    @Override
    public long getEndNumber() {
        return rangeEnd;
    }

    @Override
    public Instant getStartDate() {
        return TicksHelper.getDateFromTicks(rangeBegin);
    }

    @Override
    public Instant getEndDate() {
        return TicksHelper.getDateFromTicks(rangeEnd);
    }

    public BlockType getRangeType() {
        return rangeType;
    }

    public void setRangeBlockId(String rangeBlockId) {
        this.rangeBlockId = rangeBlockId;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public void setRangeBegin(long rangeBegin) {
        this.rangeBegin = rangeBegin;
    }

    public void setRangeEnd(long rangeEnd) {
        this.rangeEnd = rangeEnd;
    }

    public void setRangeBegin(Instant rangeBegin) {
        this.rangeBegin = TicksHelper.getTicksFromDate(rangeBegin);
    }

    public void setRangeEnd(Instant rangeEnd) {
        this.rangeEnd = TicksHelper.getTicksFromDate(rangeEnd);
    }

    public void setRangeType(BlockType rangeType) {
        this.rangeType = rangeType;
    }
}
