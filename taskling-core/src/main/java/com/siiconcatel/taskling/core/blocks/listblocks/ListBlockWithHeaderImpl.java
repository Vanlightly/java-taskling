package com.siiconcatel.taskling.core.blocks.listblocks;

import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.contexts.ListBlockWithHeaderContext;

import java.util.ArrayList;
import java.util.List;

public class ListBlockWithHeaderImpl<T,H> implements ListBlockWithHeader {
    private List<ListBlockItem<T>> items;
    private H header;
    private String listBlockId;
    private int attempt;
    private ListBlockContextImplBase<T,H> parentContext;

    public ListBlockWithHeaderImpl()
    {
        items = new ArrayList<>();
    }

    public void setParentContext(ListBlockWithHeaderContext<T,H> parentContext)
    {
        this.parentContext = (ListBlockContextImplBase<T,H>)parentContext;
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

    @Override
    public H getHeader() {
        return header;
    }

    public void setItems(List<ListBlockItem<T>> items) {
        this.items = items;
    }

    public void setHeader(H header) {
        this.header = header;
    }

    public void setListBlockId(String listBlockId) {
        this.listBlockId = listBlockId;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }
}
