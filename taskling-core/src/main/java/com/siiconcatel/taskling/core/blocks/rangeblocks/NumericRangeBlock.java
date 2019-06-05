package com.siiconcatel.taskling.core.blocks.rangeblocks;

/**
 * A NumericRangeBlock is a block which has its own status, start and end time and
 * additionally contains a numeric integer range
 */
public interface NumericRangeBlock {
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
     * The start of the numeric range
     * @return The start of the numeric range
     */
    long getStartNumber();

    /**
     * The end of the numeric range
     * @return The end of the numeric range
     */
    long getEndNumber();
}
