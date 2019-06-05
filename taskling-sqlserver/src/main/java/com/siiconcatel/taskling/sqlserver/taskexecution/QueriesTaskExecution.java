package com.siiconcatel.taskling.sqlserver.taskexecution;

class QueriesTaskExecution {
    public static final String InsertKeepAliveTaskExecution =
            "INSERT INTO [Taskling].[TaskExecution]([TaskDefinitionId],[StartedAt],[ServerName],[LastKeepAlive],[TaskDeathMode],[KeepAliveInterval],[KeepAliveDeathThreshold],\n" +
            "        [FailedTaskRetryLimit],[DeadTaskRetryLimit],[ReferenceValue],[Failed],[Blocked],[TasklingVersion],[ExecutionHeader])\n" +
            "VALUES (:taskDefinitionId, GETUTCDATE(), :serverName, GETUTCDATE(), :taskDeathMode, :keepAliveInterval, :keepAliveDeathThreshold,\n" +
            ":failedTaskRetryLimit, :deadTaskRetryLimit, :referenceValue, 0, 0,:tasklingVersion,:executionHeader);\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS INT);";

    public static final String InsertOverrideTaskExecution =
            "INSERT INTO [Taskling].[TaskExecution]([TaskDefinitionId],[StartedAt],[ServerName],[LastKeepAlive],[TaskDeathMode],[OverrideThreshold],\n" +
            "        [FailedTaskRetryLimit],[DeadTaskRetryLimit],[ReferenceValue],[Failed],[Blocked],[TasklingVersion],[ExecutionHeader])\n" +
            "VALUES (:taskDefinitionId, GETUTCDATE(), :serverName, GETUTCDATE(), :taskDeathMode, :overrideThreshold,\n" +
            ":failedTaskRetryLimit, :deadTaskRetryLimit, :referenceValue, 0, 0,:tasklingVersion,:executionHeader);\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS INT)";

    public static final String KeepAliveQuery =
            "UPDATE TE\n" +
            "SET [LastKeepAlive] = GETUTCDATE()\n" +
            "FROM [Taskling].[TaskExecution] TE\n" +
            "WHERE [TaskExecutionId] = :taskExecutionId;";

    public static final String SetCompletedDateOfTaskExecutionQuery =
            "UPDATE [Taskling].[TaskExecution]\n" +
            "SET [CompletedAt] = GETUTCDATE()\n" +
            "WHERE TaskExecutionId = :taskExecutionId";

    public static final String SetBlockedTaskExecutionQuery =
            "UPDATE [Taskling].[TaskExecution]\n" +
            "SET [Blocked] = 1\n" +
            "WHERE TaskExecutionId = :taskExecutionId";

    public static final String SetTaskExecutionAsFailedQuery =
            "UPDATE [Taskling].[TaskExecution]\n" +
            "SET [Failed] = 1\n" +
            "WHERE TaskExecutionId = :taskExecutionId";

    public static final String GetLastExecutionQuery =
            "SELECT TOP(:top) [TaskExecutionId]\n" +
            "        ,[TaskDefinitionId]\n" +
            "        ,[StartedAt]\n" +
            "        ,[CompletedAt]\n" +
            "        ,[LastKeepAlive]\n" +
            "        ,[ServerName]\n" +
            "        ,[TaskDeathMode]\n" +
            "        ,[OverrideThreshold]\n" +
            "        ,[KeepAliveInterval]\n" +
            "        ,[KeepAliveDeathThreshold]\n" +
            "        ,[FailedTaskRetryLimit]\n" +
            "        ,[DeadTaskRetryLimit]\n" +
            "        ,[ReferenceValue]\n" +
            "        ,[Failed]\n" +
            "        ,[Blocked]\n" +
            "        ,[TasklingVersion]\n" +
            "        ,[ExecutionHeader]\n" +
            "        ,GETUTCDATE() AS DbServerUtcNow\n" +
            "FROM [Taskling].[TaskExecution]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId\n" +
            "ORDER BY [TaskExecutionId] DESC";
}
