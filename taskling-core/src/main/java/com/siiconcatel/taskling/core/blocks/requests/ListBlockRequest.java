package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.ListUpdateMode;

import java.util.List;

public class ListBlockRequest extends BlockRequest {

    public ListBlockRequest() {
        super.setBlockType(BlockType.List);
    }

    private List<String> serializedValues;
    private String serializedHeader;
    private int compressionThreshold;
    private int maxStatusReasonLength;
    private int maxBlockSize;
    private ListUpdateMode listUpdateMode;
    private int uncommittedItemsThreshold;

    public List<String> getSerializedValues() {
        return serializedValues;
    }

    public void setSerializedValues(List<String> serializedValues) {
        this.serializedValues = serializedValues;
    }

    public String getSerializedHeader() {
        return serializedHeader;
    }

    public void setSerializedHeader(String serializedHeader) {
        this.serializedHeader = serializedHeader;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }

    public int getMaxStatusReasonLength() {
        return maxStatusReasonLength;
    }

    public void setMaxStatusReasonLength(int maxStatusReasonLength) {
        this.maxStatusReasonLength = maxStatusReasonLength;
    }

    public int getMaxBlockSize() {
        return maxBlockSize;
    }

    public void setMaxBlockSize(int maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
    }

    public ListUpdateMode getListUpdateMode() {
        return listUpdateMode;
    }

    public void setListUpdateMode(ListUpdateMode listUpdateMode) {
        this.listUpdateMode = listUpdateMode;
    }

    public int getUncommittedItemsThreshold() {
        return uncommittedItemsThreshold;
    }

    public void setUncommittedItemsThreshold(int uncommittedItemsThreshold) {
        this.uncommittedItemsThreshold = uncommittedItemsThreshold;
    }
}
