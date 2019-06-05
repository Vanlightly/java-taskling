package com.siiconcatel.taskling.sqlserver.tokens.criticalsections;

public class CriticalSectionQueueItem {

    private int index;
    private String taskExecutionId;

    public CriticalSectionQueueItem() {}

    public CriticalSectionQueueItem(int index, String taskExecutionId) {
        this.index = index;
        this.taskExecutionId = taskExecutionId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        this.taskExecutionId = taskExecutionId;
    }
}
