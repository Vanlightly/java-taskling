package com.siiconcatel.taskling.core.blocks.objectblocks;

/**
 * An ObjectBlock is a block which has its own status, start and end time and
 * additionally contains an object of type T. An ObjectBlock can store any arbitrary
 * data
 * @param <T> The type of the object it stores
 */
public interface ObjectBlock<T> {
    /**
     * The id of the block
     * @return The id of the block
     */
    String getObjectBlockId();

    /**
     * The attempt count. For example, if the first execution failed and the block
     * is being retried then the attempt will equal 2
     * @return attempt count
     */
    int getAttempt();

    /**
     * Returns the object
     * @return The object of Type T
     */
    T getObject();
}
