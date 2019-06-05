package com.siiconcatel.taskling.core.blocks.objectblocks;

public class ObjectBlockImpl<T> implements ObjectBlock {
    private String objectBlockId;
    private int attempt;
    private T objectData;

    public ObjectBlockImpl(String objectBlockId, int attempt, T objectData) {
        this.objectBlockId = objectBlockId;
        this.attempt = attempt;
        this.objectData = objectData;
    }

    @Override
    public String getObjectBlockId() {
        return objectBlockId;
    }

    @Override
    public int getAttempt() {
        return this.attempt;
    }

    @Override
    public Object getObject() {
        return this.objectData;
    }

    public void setObjectBlockId(String objectBlockId) {
        this.objectBlockId = objectBlockId;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public void setObjectData(T objectData) {
        this.objectData = objectData;
    }
}
