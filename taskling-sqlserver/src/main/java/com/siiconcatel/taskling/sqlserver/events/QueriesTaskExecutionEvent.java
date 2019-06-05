package com.siiconcatel.taskling.sqlserver.events;

public class QueriesTaskExecutionEvent {
    public static final String InsertTaskExecutionEventQuery =
            "INSERT INTO [Taskling].[TaskExecutionEvent]\n" +
            "        ([TaskExecutionId]\n" +
            "        ,[EventType]\n" +
            "        ,[Message]\n" +
            "        ,[EventDateTime])\n" +
            "VALUES\n" +
            "        (:taskExecutionId\n" +
            "                ,:eventType\n" +
            "                ,:message\n" +
            "                ,:eventDateTime)";
}
