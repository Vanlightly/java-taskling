package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;

public class ObjectBlockRequest<T> extends BlockRequest {

    public ObjectBlockRequest(T objectData, Class<T> objectClass){
        super.setBlockType(BlockType.Object);
        this.objectData = objectData;
        this.objectClass = objectClass;
    }

    private T objectData;
    private Class<T> objectClass;
    private int compressionThreshold;
    private boolean compressData;

    public T getObjectData() {
        return objectData;
    }

    public Class<T> getObjectClass() {
        return objectClass;
    }

    public void setObjectClass(Class<T> objectClass) {
        this.objectClass = objectClass;
    }

    public void setObjectData(T objectData) {
        this.objectData = objectData;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }

    public boolean shouldCompressData() {
        return compressData;
    }

    public void setCompressData(boolean compressData) {
        this.compressData = compressData;
    }
}
