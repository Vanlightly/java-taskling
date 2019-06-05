package com.siiconcatel.taskling.core.tasks;

public enum TaskExecutionStatus
{
    NotDefined(0),
    Completed(1),
    InProgress(2),
    Dead(3),
    Failed(4),
    Blocked(5);

    private int numVal;

    TaskExecutionStatus(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }
}
