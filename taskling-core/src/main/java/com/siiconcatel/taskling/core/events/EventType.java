package com.siiconcatel.taskling.core.events;

import java.util.HashMap;
import java.util.Map;

public enum EventType {
    NotDefined(0),
    Start(1),
    CheckPoint(2),
    Error(3),
    End(4),
    Blocked(5);

    private int numVal;

    EventType(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }


    private static Map map = new HashMap<>();
    public static EventType valueOf(int blockType) {
        return (EventType) map.get(blockType);
    }

    static {
        for (EventType eventType : EventType.values()) {
            map.put(eventType.numVal, eventType);
        }
    }
}
