package com.siiconcatel.taskling.core.blocks.listblocks;

import java.util.HashMap;
import java.util.Map;

public enum ItemStatus {
    NotDefined(0),
    Pending(1),
    Completed(2),
    Failed(3),
    Discarded(4),
    All(5);

    private int numVal;

    ItemStatus(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }

    private static Map map = new HashMap<>();
    public static ItemStatus valueOf(int numVal) {
        return (ItemStatus) map.get(numVal);
    }

    static {
        for (ItemStatus item : ItemStatus.values()) {
            map.put(item.numVal, item);
        }
    }
}
