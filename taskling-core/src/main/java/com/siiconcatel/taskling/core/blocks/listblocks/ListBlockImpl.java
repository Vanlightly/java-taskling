package com.siiconcatel.taskling.core.blocks.listblocks;

import com.siiconcatel.taskling.core.contexts.ListBlockContext;

import java.util.ArrayList;
import java.util.List;

public class ListBlockImpl<T> implements ListBlock {
    private List<ListBlockItem<T>> items;
    private String listBlockId;
    private int attempt;
    private ListBlockContextImplBase<T,String> parentContext;

    public ListBlockImpl()
    {
        items = new ArrayList<>();
    }

    public void setParentContext(ListBlockContext parentContext)
    {
        this.parentContext = (ListBlockContextImplBase<T,String>)parentContext;
    }

    @Override
    public String getListBlockId() {
        return listBlockId;
    }

    @Override
    public int getAttempt() {
        return attempt;
    }

    @Override
    public List<ListBlockItem<T>> getItems()
    {
        if (items == null || items.isEmpty())
        {
            if (parentContext != null)
                parentContext.fillItems();
        }

        return items;
    }

    public void setItems(List<ListBlockItem<T>> items) {
        this.items = items;
    }

    public void setListBlockId(String listBlockId) {
        this.listBlockId = listBlockId;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }
}
