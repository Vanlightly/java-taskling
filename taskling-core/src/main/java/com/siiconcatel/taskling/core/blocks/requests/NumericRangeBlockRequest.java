package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;

import java.util.Optional;

public class NumericRangeBlockRequest extends BlockRequest {
    public NumericRangeBlockRequest() {
        super.setBlockType(BlockType.NumericRange);
    }

    private Long rangeBegin;
    private Long rangeEnd;
    private Long blockSize;

    public Optional<Long> getRangeBegin() {
        if(rangeBegin == null)
            return Optional.empty();
        return Optional.of(rangeBegin);
    }

    public void setRangeBegin(long rangeBegin) {
        this.rangeBegin = rangeBegin;
    }

    public Optional<Long> getRangeEnd()
    {
        if(rangeEnd == null)
            return Optional.empty();
        return Optional.of(rangeEnd);
    }

    public void setRangeEnd(long rangeEnd) {
        this.rangeEnd = rangeEnd;
    }

    public Optional<Long> getBlockSize()
    {
        if(blockSize == null)
            return Optional.empty();
        return Optional.of(blockSize);
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }
}
