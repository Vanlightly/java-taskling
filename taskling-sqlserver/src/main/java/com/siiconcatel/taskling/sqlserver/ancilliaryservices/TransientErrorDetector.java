package com.siiconcatel.taskling.sqlserver.ancilliaryservices;

import java.sql.SQLException;

public class TransientErrorDetector
{
    public static boolean isTransient(SQLException sqlEx)
    {
        if (sqlEx.getErrorCode() == 1205 // 1205 = Deadlock
                || sqlEx.getErrorCode() == -2 // -2 = TimeOut
                || sqlEx.getErrorCode() == -1 // -1 = Connection
                || sqlEx.getErrorCode() == 2 // 2 = Connection
                || sqlEx.getErrorCode() == 53 // 53 = Connection
        ) {
            return true;
        }

        return false;
    }
}
