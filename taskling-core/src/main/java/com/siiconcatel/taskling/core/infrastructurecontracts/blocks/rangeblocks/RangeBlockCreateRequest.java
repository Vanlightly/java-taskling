package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.rangeblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.utils.TicksHelper;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

import java.time.Instant;

public class RangeBlockCreateRequest extends BlockRequestBase {
    public RangeBlockCreateRequest(TaskId taskId,
                                   String taskExecutionId,
                                   Instant fromDate,
                                   Instant toDate)
    {
        super(taskId, taskExecutionId, BlockType.DateRange);
        from = TicksHelper.getTicksFromDate(fromDate);
        to = TicksHelper.getTicksFromDate(toDate);
    }

    public RangeBlockCreateRequest(TaskId taskId,
                                   String taskExecutionId,
                                   long from,
                                   long to)
    {
        super(taskId, taskExecutionId, BlockType.NumericRange);
        this.from = from;
        this.to = to;
    }

    private long from;
    private long to;

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }
}
