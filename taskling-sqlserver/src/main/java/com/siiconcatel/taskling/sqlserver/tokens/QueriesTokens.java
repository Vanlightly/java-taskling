package com.siiconcatel.taskling.sqlserver.tokens;

public class QueriesTokens {
    public final static String AcquireLockQuery =
            "UPDATE Taskling.[TaskDefinition]\n" +
            "SET [HoldLockTaskExecutionId] = :taskExecutionId\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    public final static String GetTaskExecutionsBaseQuery =
            "SELECT [TaskExecutionId]\n" +
            "        ,[StartedAt]\n" +
            "        ,[CompletedAt]\n" +
            "        ,[LastKeepAlive]\n" +
            "        ,[TaskDeathMode]\n" +
            "        ,[OverrideThreshold]\n" +
            "        ,[KeepAliveInterval]\n" +
            "        ,[KeepAliveDeathThreshold]\n" +
            "        ,GETUTCDATE() AS [CurrentDateTime]\n" +
            "FROM [Taskling].[TaskExecution]\n" +
            "WHERE [TaskExecutionId] IN ";

    public final static String GetTaskExecutions(int taskExecutionCount)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(GetTaskExecutionsBaseQuery);

        sb.append("(");
        for (int i = 0; i < taskExecutionCount; i++)
        {
            if (i > 0)
                sb.append(",");

            sb.append(":inParam" + i);
        }

        sb.append(")");

        return sb.toString();
    }

    public final static String GetUserCriticalSectionStateQuery =
            "SELECT [UserCsStatus]\n" +
            "        ,[UserCsTaskExecutionId]\n" +
            "        ,[UserCsQueue]\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    public final static String GetClientCriticalSectionStateQuery =
            "SELECT [ClientCsStatus]\n" +
            "        ,[ClientCsTaskExecutionId]\n" +
            "        ,[ClientCsQueue]\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    public final static String SetUserCriticalSectionStateQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [UserCsStatus] = :csStatus\n" +
            "  ,[UserCsTaskExecutionId] = :csTaskExecutionId\n" +
            "  ,[UserCsQueue] = :csQueue\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    public final static String SetClientCriticalSectionStateQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [ClientCsStatus] = :csStatus\n" +
            "  ,[ClientCsTaskExecutionId] = :csTaskExecutionId\n" +
            "  ,[ClientCsQueue] = :csQueue\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    public final static String ReturnUserCriticalSectionTokenQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [UserCsStatus] = 1\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    public final static String ReturnClientCriticalSectionTokenQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [ClientCsStatus] = 1\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId;";

    public final static String GetExecutionTokensQuery =
            "SELECT [ExecutionTokens]\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE TaskDefinitionId = :taskDefinitionId";

    public final static String UpdateExecutionTokensQuery =
            "UPDATE [Taskling].[TaskDefinition]\n" +
            "SET [ExecutionTokens] = :executionTokens\n" +
            "WHERE TaskDefinitionId = :taskDefinitionId";
}
