package com.siiconcatel.taskling.sqlserver.ancilliaryservices;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

public class NullableField {

    public static Integer getInteger(ResultSet rs, String strColName) throws SQLException {
            int value = rs.getInt(strColName);
            return rs.wasNull() ? null : value;
    }

    public static Instant getInstant(ResultSet rs, String strColName) throws SQLException {
        Timestamp value = rs.getTimestamp(strColName);
        return rs.wasNull() ? null : TimeHelper.toInstant(value);
    }

    public static Instant getInstant(ResultSet rs, String strColName, Instant defaultValue) throws SQLException {
        Timestamp value = rs.getTimestamp(strColName);
        return rs.wasNull() ? defaultValue : TimeHelper.toInstant(value);
    }

    public static Duration getDuration(ResultSet rs, String strColName) throws SQLException {
        String value = rs.getString(strColName);

        if(rs.wasNull())
            return null;

        int hours=0;
        int minutes=0;
        int seconds = 0;

        int nanoDotIndex = value.indexOf(".");


        String[] parts = value.substring(0,nanoDotIndex).split("\\:");
        hours = Integer.parseInt(parts[0]);
        minutes = Integer.parseInt(parts[1]);
        seconds = Integer.parseInt(parts[2]);

        int totalSeconds = (60 * 60 * hours)
                + (60 * minutes)
                + seconds;

        return Duration.ofSeconds(totalSeconds);
    }

    public static String getString(ResultSet rs, String strColName) throws SQLException {
        String value = rs.getString(strColName);
        return rs.wasNull() ? null : value;
    }
}
