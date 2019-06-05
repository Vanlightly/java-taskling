package com.siiconcatel.taskling.core.contexts;

import com.siiconcatel.taskling.core.blocks.listblocks.ItemStatus;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlock;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockItem;

import java.util.List;
import java.util.Optional;

/**
 * A block context that stores a list of items which can be tracked
 */
public interface ListBlockContext<T> extends BlockContext {
    /**
     * Returns the inner list block
     * @return ListBlock
     */
    ListBlock<T> getBlock();

    /**
     * Returns the id of the block
     * @return Block id
     */
    String getListBlockId();

    /**
     * If the context was created by adding its id to the force block queue, this id has a non zero value.
     * @return The id of forced block queue entry that triggered the creation of this context
     */
    Optional<String> getForcedBlockQueueId();

    /**
     * Returns the items of the list block. A list block is often loaded lazily so calling this the first time
     * method loads the items from the backend store
     * @param statuses Filters the items to return
     * @return List of ListBlockItem
     */
    List<ListBlockItem<T>> getItems(ItemStatus... statuses);

    /**
     * Marks the ListBlockItem as complete
     * @param item
     */
    void itemComplete(ListBlockItem<T> item);

    /**
     * Marks the ListBlockItem as failed
     * @param item
     * @param reason The reason for the failure
     */
    void itemFailed(ListBlockItem<T> item, String reason);

    /**
     * Marks the ListBlockItem as discarded
     * @param item
     * @param reason The reason for discarding the item
     */
    void discardItem(ListBlockItem<T> item, String reason);

    /**
     * Returns only the data values of the ListBlockItems
     * @param statuses Filters the values returned by status value
     * @return List of data values
     */
    List<T> getItemValues(ItemStatus... statuses);

    /**
     * Causes all items that have pending status changes to be updated
     * in the backend store.
     */
    void flush();
}
