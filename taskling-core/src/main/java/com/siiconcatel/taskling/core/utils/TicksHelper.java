package com.siiconcatel.taskling.core.utils;

import java.time.Instant;

public class TicksHelper {
    private static final long TICKS_AT_EPOCH = 621355968000000000L;
    private static final long TICKS_PER_MILLISECOND = 10000;

    public static Instant getDateFromTicks(long dateInTicks) {
        return Instant.ofEpochMilli((dateInTicks - TICKS_AT_EPOCH) / TICKS_PER_MILLISECOND);
    }

    public static long getTicksFromDate(Instant date) {
        return (date.toEpochMilli() * TICKS_PER_MILLISECOND) + TICKS_AT_EPOCH;
    }
}
