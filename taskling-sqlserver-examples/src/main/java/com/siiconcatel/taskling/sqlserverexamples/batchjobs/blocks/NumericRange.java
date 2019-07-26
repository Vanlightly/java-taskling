package com.siiconcatel.taskling.sqlserverexamples.batchjobs.blocks;

public class NumericRange {
    private long from;
    private long to;

    public NumericRange(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public long getFrom() {
        return from;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public void setTo(long to) {
        this.to = to;
    }
}
