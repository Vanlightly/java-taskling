package com.siiconcatel.taskling.core.blocks.listblocks;

import java.util.List;

/**
 * A ListBlockWithHeader is a block which has a header, its own status, start and end time and
 * additionally contains N items, each of which has a status, start and end time
 * @param <T> the type of its items
 * @param <H> the type of its header
 */
public interface ListBlockWithHeader<T,H> {
    /**
     * The id of the block
     * @return The id of the block
     */
    String getListBlockId();

    /**
     * The attempt count. For example, if the first execution failed and the block
     * is being retried then the attempt will equal 2
     * @return attempt count
     */
    int getAttempt();

    /**
     * Returns the header
     * @return The header
     */
    H getHeader();

    /**
     * The items of the block
     * @return List of ListBlockItem
     */
    List<ListBlockItem<T>> getItems();
}
