package com.siiconcatel.taskling.sqlserver.taskexecution;

class QueriesTask {
    public static final String GetTaskQuery =
            "SELECT [ApplicationName]\n" +
            "        ,[TaskName]\n" +
            "        ,[TaskDefinitionId]\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE [ApplicationName] = :applicationName\n" +
            "AND [TaskName] = :taskName";

    public static final String GetLastCleanUpTimeQuery =
            "SELECT [LastCleaned]\n" +
            "FROM [Taskling].[TaskDefinition] WITH(NOLOCK)\n" +
            "WHERE [ApplicationName] = :applicationName\n" +
            "AND [TaskName] = :taskName";

    public static final String SetLastCleanUpTimeQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [LastCleaned] = GETUTCDATE()\n" +
            "WHERE [ApplicationName] = :applicationName\n" +
            "AND [TaskName] = :taskName";

    public static final String InsertTaskQuery =
            "INSERT INTO [Taskling].[TaskDefinition]([ApplicationName],[TaskName],[LastCleaned],[UserCsStatus],[ClientCsStatus])\n" +
            "VALUES(:applicationName,:taskName,GETUTCDATE(),1,1)\n" +
            "\n" +
            "SELECT TaskDefinitionId\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "AND TaskName = :taskName";
}
