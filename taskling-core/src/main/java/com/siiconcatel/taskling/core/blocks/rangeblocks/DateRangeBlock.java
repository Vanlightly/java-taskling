package com.siiconcatel.taskling.core.blocks.rangeblocks;

import java.time.Instant;

/**
 * A DateRangeBlock is a block which has its own status, start and end time and
 * additionally contains a UTC date range
 */
public interface DateRangeBlock {

    /**
     * The id of the block
     * @return The id of the block
     */
    String getRangeBlockId();

    /**
     * The attempt count. For example, if the first execution failed and the block
     * is being retried then the attempt will equal 2
     * @return attempt count
     */
    int getAttempt();

    /**
     * The UTC start of the date range
     * @return UTC date
     */
    Instant getStartDate();

    /**
     * The UTC end of the date range
     * @return UTC date
     */
    Instant getEndDate();
}
