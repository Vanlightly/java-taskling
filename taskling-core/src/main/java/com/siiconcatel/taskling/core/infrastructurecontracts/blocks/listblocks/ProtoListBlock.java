package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks;

import java.util.List;

public class ProtoListBlock {
    private String listBlockId;
    private int attempt;
    private String header;
    private boolean isForcedBlock;
    private int forcedBlockQueueId;
    private List<ProtoListBlockItem> items;

    public ProtoListBlock(String listBlockId, int attempt) {
        this.listBlockId = listBlockId;
        this.attempt = attempt;
    }

    public ProtoListBlock(String listBlockId, int attempt, String header) {
        this.listBlockId = listBlockId;
        this.attempt = attempt;
        this.header = header;
    }

    public String getListBlockId() {
        return listBlockId;
    }

    public void setListBlockId(String listBlockId) {
        this.listBlockId = listBlockId;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public boolean isForcedBlock() {
        return isForcedBlock;
    }

    public void setForcedBlock(boolean forcedBlock) {
        isForcedBlock = forcedBlock;
    }

    public int getForcedBlockQueueId() {
        return forcedBlockQueueId;
    }

    public void setForcedBlockQueueId(int forcedBlockQueueId) {
        this.forcedBlockQueueId = forcedBlockQueueId;
    }

    public List<ProtoListBlockItem> getItems() {
        return items;
    }

    public void setItems(List<ProtoListBlockItem> items) {
        this.items = items;
    }
}
