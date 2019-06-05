package com.siiconcatel.taskling.core.tasks;

import java.util.HashMap;
import java.util.Map;

public enum TaskDeathMode
{
    //TODO verify if having changed the order is compatible with .NET version
    NotDefined(0),
    KeepAlive(1),
    Override(2);

    private int numVal;

    TaskDeathMode(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }

    private static Map map = new HashMap<>();
    public static TaskDeathMode valueOf(int numVal) {
        return (TaskDeathMode) map.get(numVal);
    }

    static {
        for (TaskDeathMode item : TaskDeathMode.values()) {
            map.put(item.numVal, item);
        }
    }
}
