package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

public enum GrantStatus {
    Denied(0),
    Granted(1);

    private int numVal;

    GrantStatus(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }
}
