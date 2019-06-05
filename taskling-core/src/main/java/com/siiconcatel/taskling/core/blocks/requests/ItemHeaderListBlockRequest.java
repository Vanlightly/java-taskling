package com.siiconcatel.taskling.core.blocks.requests;

public class ItemHeaderListBlockRequest<T,H> extends ListBlockRequest {

    private Class<T> itemType;
    private Class<H> headerType;

    public ItemHeaderListBlockRequest(Class<T> itemType, Class<H> headerType) {
        super();
        this.itemType = itemType;
        this.headerType = headerType;
    }

    public Class<T> getItemType() {
        return itemType;
    }

    public void setItemType(Class<T> itemType) {
        this.itemType = itemType;
    }

    public Class<H> getHeaderType() {
        return headerType;
    }

    public void setHeaderType(Class<H> headerType) {
        this.headerType = headerType;
    }
}
