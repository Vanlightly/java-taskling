package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks;

public class ListBlockCreateResponse {
    private ProtoListBlock block;

    public ListBlockCreateResponse(ProtoListBlock block) {
        this.block = block;
    }

    public ProtoListBlock getBlock() {
        return block;
    }

    public void setBlock(ProtoListBlock block) {
        this.block = block;
    }
}
