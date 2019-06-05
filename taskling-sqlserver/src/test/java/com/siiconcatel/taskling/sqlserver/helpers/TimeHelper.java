package com.siiconcatel.taskling.sqlserver.helpers;

import java.time.*;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeHelper {
    public static Instant getInstant(int year, int month, int dayOfMonth) {
        return LocalDateTime.of(year, month, dayOfMonth, 0, 0, 0)
                .atZone(ZoneOffset.UTC).toInstant();
    }

    public static Instant getInstant(int year, int month, int dayOfMonth, int hour, int minute, int second) {
        return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second)
                .atZone(ZoneOffset.UTC).toInstant();
    }

    public static Date getDate(int year, int month, int day) {
        Calendar myCalendar = new GregorianCalendar(year, month, day);
        return myCalendar.getTime();
    }

    public static Date addMinutes(Date date, int minutes) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, minutes);
        return calendar.getTime();
    }
}
