package com.siiconcatel.taskling.core.blocks.listblocks;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;

public class ListBlockItemImpl<T> implements ListBlockItem {
    private Consumer<ListBlockItem<T>> itemComplete;
    private Consumer<ListBlockItemActionArgs<T>> itemFailed;
    private Consumer<ListBlockItemActionArgs<T>> discardItem;

    private String listBlockItemId;
    private T value;
    private ItemStatus status;
    private String statusReason;
    private Instant lastUpdated;

    public ListBlockItemImpl(String listBlockItemId,
                             T value,
                             ItemStatus status,
                             String statusReason,
                             Instant lastUpdated) {
        this.listBlockItemId = listBlockItemId;
        this.value = value;
        this.status = status;
        this.statusReason = statusReason;
        this.lastUpdated = lastUpdated;
    }

    public void setParentContext(Consumer<ListBlockItem<T>> itemComplete,
                                 Consumer<ListBlockItemActionArgs<T>> itemFailed,
                                 Consumer<ListBlockItemActionArgs<T>> discardItem)
    {
        this.itemComplete = itemComplete;
        this.itemFailed = itemFailed;
        this.discardItem = discardItem;
    }

    @Override
    public String getListBlockItemId() {
        return listBlockItemId;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public ItemStatus getStatus() {
        return status;
    }

    @Override
    public String getStatusReason() {
        return statusReason;
    }

    @Override
    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public void setStatus(ItemStatus status) {
        this.status = status;
    }

    public void setStatusReason(String statusReason) {
        this.statusReason = statusReason;
    }

    public void completed()
    {
        itemComplete.accept(this);
    }
    public void failed(String message)
    {
        ListBlockItemActionArgs args = new ListBlockItemActionArgs(this, message);
        itemFailed.accept(args);
    }

    public void discarded(String message)
    {
        ListBlockItemActionArgs args = new ListBlockItemActionArgs(this, message);
        discardItem.accept(args);
    }
}
