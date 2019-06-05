package com.siiconcatel.taskling.sqlserver.tokens.executions;

import java.util.HashMap;
import java.util.Map;

public enum ExecutionTokenStatus {
    Unavailable(0),
    Available(1),
    Disabled(2),
    Unlimited(3);

    private int numVal;

    ExecutionTokenStatus(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }

    private static Map map = new HashMap<>();
    public static ExecutionTokenStatus valueOf(int numVal) {
        return (ExecutionTokenStatus) map.get(numVal);
    }

    static {
        for (ExecutionTokenStatus item : ExecutionTokenStatus.values()) {
            map.put(item.numVal, item);
        }
    }
}
