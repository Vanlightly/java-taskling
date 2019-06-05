package com.siiconcatel.taskling.core.blocks.listblocks;

public enum ListUpdateMode
{
    SingleItemCommit(0),
    PeriodicBatchCommit(1),
    BatchCommitAtEnd(2);

    private int numVal;

    ListUpdateMode(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }
}
