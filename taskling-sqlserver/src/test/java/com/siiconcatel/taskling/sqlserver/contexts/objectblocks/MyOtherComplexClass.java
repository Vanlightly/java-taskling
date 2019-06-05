package com.siiconcatel.taskling.sqlserver.contexts.objectblocks;

import java.util.List;

public class MyOtherComplexClass {
    private double value;
    private List<String> notes;

    public MyOtherComplexClass() {
    }

    public MyOtherComplexClass(double value, List<String> notes) {
        this.value = value;
        this.notes = notes;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public List<String> getNotes() {
        return notes;
    }

    public void setNotes(List<String> notes) {
        this.notes = notes;
    }
}
