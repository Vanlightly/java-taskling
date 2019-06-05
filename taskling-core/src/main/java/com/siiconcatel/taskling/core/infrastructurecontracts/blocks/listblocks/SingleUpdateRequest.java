package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

public class SingleUpdateRequest {
    private TaskId taskId;
    private String listBlockId;
    private ProtoListBlockItem listBlockItem;

    public SingleUpdateRequest(TaskId taskId, String listBlockId, ProtoListBlockItem listBlockItem) {
        this.taskId = taskId;
        this.listBlockId = listBlockId;
        this.listBlockItem = listBlockItem;
    }

    public TaskId getTaskId() {
        return taskId;
    }

    public void setTaskId(TaskId taskId) {
        this.taskId = taskId;
    }

    public String getListBlockId() {
        return listBlockId;
    }

    public void setListBlockId(String listBlockId) {
        this.listBlockId = listBlockId;
    }

    public ProtoListBlockItem getListBlockItem() {
        return listBlockItem;
    }

    public void setListBlockItem(ProtoListBlockItem listBlockItem) {
        this.listBlockItem = listBlockItem;
    }
}
