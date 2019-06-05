package com.siiconcatel.taskling.core.blocks.common;

public enum LastBlockOrder {
    LastCreated(0),
    RangeStart(1),
    RangeEnd(2);

    private int numVal;

    LastBlockOrder(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }
}
