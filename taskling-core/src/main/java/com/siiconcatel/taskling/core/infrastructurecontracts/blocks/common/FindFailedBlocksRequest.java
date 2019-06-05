package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

import java.time.Instant;

public class FindFailedBlocksRequest extends BlockRequestBase {
    public FindFailedBlocksRequest(TaskId taskId,
                                   String taskExecutionId,
                                   BlockType blockType,
                                   Instant searchPeriodBegin,
                                   Instant searchPeriodEnd,
                                   int blockCountLimit,
                                   int retryLimit)
    {
        super(taskId, taskExecutionId, blockType);
        this.searchPeriodBegin = searchPeriodBegin;
        this.searchPeriodEnd = searchPeriodEnd;
        this.blockCountLimit = blockCountLimit;
        this.retryLimit = retryLimit;
    }


    private Instant searchPeriodBegin;
    private Instant searchPeriodEnd;
    private int blockCountLimit;
    private int retryLimit;

    public Instant getSearchPeriodBegin() {
        return searchPeriodBegin;
    }

    public void setSearchPeriodBegin(Instant searchPeriodBegin) {
        this.searchPeriodBegin = searchPeriodBegin;
    }

    public Instant getSearchPeriodEnd() {
        return searchPeriodEnd;
    }

    public void setSearchPeriodEnd(Instant searchPeriodEnd) {
        this.searchPeriodEnd = searchPeriodEnd;
    }

    public int getBlockCountLimit() {
        return blockCountLimit;
    }

    public void setBlockCountLimit(int blockCountLimit) {
        this.blockCountLimit = blockCountLimit;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }
}
