package com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections;

public enum CriticalSectionType {
    User(0),
    Client(1);

    private int numVal;

    CriticalSectionType(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }
}
