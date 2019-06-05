package com.siiconcatel.taskling.core.blocks.listblocks;

import java.util.Optional;

public class ListBlockItemActionArgs<T> {
    private ListBlockItem listBlockItem;
    private String message;

    public ListBlockItemActionArgs(ListBlockItem listBlockItem, String message) {
        this.listBlockItem = listBlockItem;
        this.message = message;
    }

    public ListBlockItem getListBlockItem() {
        return listBlockItem;
    }

    public void setListBlockItem(ListBlockItem listBlockItem) {
        this.listBlockItem = listBlockItem;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}
