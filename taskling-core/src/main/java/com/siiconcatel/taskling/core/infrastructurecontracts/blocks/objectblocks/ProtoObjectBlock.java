package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks;

public class ProtoObjectBlock {
    private String objectBlockId;
    int attempt;
    String objectData;

    public ProtoObjectBlock(String objectBlockId, int attempt, String objectData) {
        this.objectBlockId = objectBlockId;
        this.attempt = attempt;
        this.objectData = objectData;
    }

    public String getObjectBlockId() {
        return objectBlockId;
    }

    public void setObjectBlockId(String objectBlockId) {
        this.objectBlockId = objectBlockId;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public String getObjectData() {
        return objectData;
    }

    public void setObjectData(String objectData) {
        this.objectData = objectData;
    }
}
