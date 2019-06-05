package com.siiconcatel.taskling.sqlserver.contexts;

import java.util.Date;

public class TestHeader {
    private String purchaseCode;
    private Date fromDate;
    private Date toDate;

    public TestHeader() {
    }

    public TestHeader(String purchaseCode, Date fromDate, Date toDate) {
        this.purchaseCode = purchaseCode;
        this.fromDate = fromDate;
        this.toDate = toDate;
    }

    public String getPurchaseCode() {
        return purchaseCode;
    }

    public void setPurchaseCode(String purchaseCode) {
        this.purchaseCode = purchaseCode;
    }

    public Date getFromDate() {
        return fromDate;
    }

    public void setFromDate(Date fromDate) {
        this.fromDate = fromDate;
    }

    public Date getToDate() {
        return toDate;
    }

    public void setToDate(Date toDate) {
        this.toDate = toDate;
    }
}
