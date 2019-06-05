package com.siiconcatel.taskling.sqlserver.helpers;

import com.siiconcatel.taskling.core.events.EventType;

public class LastEvent {
    public final EventType Type;
    public final String Description;

    public LastEvent(EventType type, String description) {
        Type = type;
        Description = description;
    }
}
