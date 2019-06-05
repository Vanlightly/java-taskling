package com.siiconcatel.taskling.sqlserver.contexts.objectblocks;

import java.util.Date;

public class MyComplexClass {
    private int id;
    private String name;
    private Date dateOfBirth;
    private MyOtherComplexClass someOtherData;


    public MyComplexClass() {
    }

    public MyComplexClass(int id, String name, Date dateOfBirth, MyOtherComplexClass someOtherData) {
        this.id = id;
        this.name = name;
        this.dateOfBirth = dateOfBirth;
        this.someOtherData = someOtherData;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(Date dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public MyOtherComplexClass getSomeOtherData() {
        return someOtherData;
    }

    public void setSomeOtherData(MyOtherComplexClass someOtherData) {
        this.someOtherData = someOtherData;
    }
}
