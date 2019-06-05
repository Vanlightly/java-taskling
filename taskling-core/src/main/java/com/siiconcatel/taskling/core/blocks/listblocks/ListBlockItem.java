package com.siiconcatel.taskling.core.blocks.listblocks;

import java.time.Instant;
import java.util.Optional;

/**
 * Each ListBlockItem has data and a status, and belongs to a ListBlock
 * @param <T> The type of each item
 */
public interface ListBlockItem<T> {
    /**
     * The id of the ListBlockItem
     * @return The id of the ListBlockItem
     */
    String getListBlockItemId();

    /**
     * The data value of the item
     * @return The data value of the item
     */
    T getValue();

    /**
     * The status of the item
     * @return The status of the item
     */
    ItemStatus getStatus();

    /**
     * The reason for its current status (if set)
     * @return
     */
    String getStatusReason();

    /**
     * The UTC time the item was last updated. This could be from a previous execution
     * @return Time of last update
     */
    Instant getLastUpdated();

    /**
     * Changes the status of the item to completed
     */
    void completed();

    /**
     * Changes the status of the item to failed
     * @param message The accompanying reason for the failure
     */
    void failed(String message);

    /**
     * Changes the status of the item to discarded
     * @param message The accompanying reason for discarding the item
     */
    void discarded(String message);
}
