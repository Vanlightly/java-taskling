package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks;

import com.siiconcatel.taskling.core.blocks.listblocks.ItemStatus;

import java.time.Instant;
import java.util.Optional;

public class ProtoListBlockItem {

    private String listBlockItemId;
    private String value;
    private ItemStatus status;
    private String statusReason;
    private Instant lastUpdated;

    public ProtoListBlockItem() {}

    public ProtoListBlockItem(String listBlockItemId, String value, ItemStatus status, String statusReason, Instant lastUpdated) {
        this.listBlockItemId = listBlockItemId;
        this.value = value;
        this.status = status;
        this.statusReason = statusReason;
        this.lastUpdated = lastUpdated;
    }

    public String getListBlockItemId() {
        return listBlockItemId;
    }

    public void setListBlockItemId(String listBlockItemId) {
        this.listBlockItemId = listBlockItemId;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ItemStatus getStatus() {
        return status;
    }

    public void setStatus(ItemStatus status) {
        this.status = status;
    }

    public String getStatusReason() {
        return statusReason;
    }

    public void setStatusReason(String statusReason) {
        this.statusReason = statusReason;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
