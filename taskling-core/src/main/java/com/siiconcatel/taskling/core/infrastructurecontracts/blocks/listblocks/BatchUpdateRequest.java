package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

import java.util.List;

public class BatchUpdateRequest {
    private TaskId taskId;
    private String listBlockId;
    private List<ProtoListBlockItem> listBlockItems;

    public BatchUpdateRequest(TaskId taskId, String listBlockId, List<ProtoListBlockItem> listBlockItems) {
        this.taskId = taskId;
        this.listBlockId = listBlockId;
        this.listBlockItems = listBlockItems;
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

    public List<ProtoListBlockItem> getListBlockItems() {
        return listBlockItems;
    }

    public void setListBlockItems(List<ProtoListBlockItem> listBlockItems) {
        this.listBlockItems = listBlockItems;
    }
}
