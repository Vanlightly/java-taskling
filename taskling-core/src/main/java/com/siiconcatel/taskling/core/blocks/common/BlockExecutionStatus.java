package com.siiconcatel.taskling.core.blocks.common;

public enum BlockExecutionStatus {
    NotDefined(0),
    NotStarted(1),
    Started(2),
    Completed(3),
    Failed(4);

    private int numVal;

    BlockExecutionStatus(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }
}
