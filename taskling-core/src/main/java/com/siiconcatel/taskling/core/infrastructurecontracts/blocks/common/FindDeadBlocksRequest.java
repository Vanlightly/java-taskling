package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;

import java.time.Instant;

public class FindDeadBlocksRequest extends BlockRequestBase {
    public FindDeadBlocksRequest(TaskId taskId,
                                 String taskExecutionId,
                                 BlockType blockType,
                                 Instant searchPeriodBegin,
                                 Instant searchPeriodEnd,
                                 int blockCountLimit,
                                 TaskDeathMode taskDeathMode,
                                 int retryLimit)
    {
        super(taskId, taskExecutionId, blockType);
        this.searchPeriodBegin = searchPeriodBegin;
        this.searchPeriodEnd = searchPeriodEnd;
        this.blockCountLimit = blockCountLimit;
        this.taskDeathMode = taskDeathMode;
        this.retryLimit = retryLimit;
    }

    private Instant searchPeriodBegin;
    private Instant searchPeriodEnd;
    private int blockCountLimit;
    private TaskDeathMode taskDeathMode;
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

    public TaskDeathMode getTaskDeathMode() {
        return taskDeathMode;
    }

    public void setTaskDeathMode(TaskDeathMode taskDeathMode) {
        this.taskDeathMode = taskDeathMode;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }
}
