package com.siiconcatel.taskling.core.blocks.listblocks;

public enum BatchSize {
    NotSet(0), Ten(10), Fifty(50), Hundred(100), FiveHundred(500);

    private int numVal;

    BatchSize(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }
}
