package com.siiconcatel.taskling.sqlserver.ancilliaryservices;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;

public class TimeHelper {
    public static Instant toInstant(java.sql.Timestamp ts) {
        return ts.toLocalDateTime().toInstant(ZoneOffset.UTC);
    }

    public static java.sql.Timestamp toTimestamp(Instant instant) {
        return Timestamp.valueOf(instant.atZone(ZoneOffset.UTC).toLocalDateTime());
    }
}
