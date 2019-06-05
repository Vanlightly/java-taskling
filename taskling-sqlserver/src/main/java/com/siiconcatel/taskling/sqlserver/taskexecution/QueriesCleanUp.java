package com.siiconcatel.taskling.sqlserver.taskexecution;

public class QueriesCleanUp {
    public final static String IdentifyOldBlocksQuery =
            "SELECT [BlockId]\n" +
            "FROM [Taskling].[Block] WITH(NOLOCK)\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId\n" +
            "AND [CreatedDate] < :olderThanDate";

    public final static String DeleteListItemsOfBlockQuery =
            "DELETE FROM [Taskling].[ListBlockItem]\n" +
            "WHERE BlockId = :blockId";

    public final static String DeleteOldDataQuery =
            "DELETE FROM [Taskling].[BlockExecution]\n" +
            "WHERE BlockExecutionId IN (\n" +
            "   SELECT BlockExecutionId\n" +
            "    FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)\n" +
            "    LEFT JOIN [Taskling].[Block] B WITH(NOLOCK) ON BE.BlockId = B.BlockId\n" +
            "    WHERE (B.TaskDefinitionId = :taskDefinitionId\n" +
            "                AND B.CreatedDate < :olderThanDate)\n" +
            "    OR B.TaskDefinitionId IS NULL);\n" +
            "\n" +
            "DELETE FROM [Taskling].[Block]\n" +
            "WHERE BlockId IN (\n" +
            "        SELECT BlockId\n" +
            "    FROM [Taskling].[Block] WITH(NOLOCK)\n" +
            "    WHERE TaskDefinitionId = :taskDefinitionId\n" +
            "    AND CreatedDate < :olderThanDate);\n" +
            "\n" +
            "DELETE FROM [Taskling].[TaskExecutionEvent]\n" +
            "WHERE TaskExecutionEventId IN (\n" +
            "    SELECT TaskExecutionEventId\n" +
            "    FROM [Taskling].[TaskExecutionEvent] TEE WITH(NOLOCK)\n" +
            "    LEFT JOIN [Taskling].[TaskExecution] TE WITH(NOLOCK) ON TEE.TaskExecutionId  = TE.TaskExecutionId\n" +
            "    WHERE (TE.TaskDefinitionId = :taskDefinitionId\n" +
            "                AND TE.StartedAt < :olderThanDate)\n" +
            "    OR TE.TaskDefinitionId IS NULL);\n" +
            "\n" +
            "DELETE FROM [Taskling].[TaskExecution]\n" +
            "WHERE TaskExecutionId IN (\n" +
            "    SELECT TaskExecutionId\n" +
            "    FROM [Taskling].[TaskExecution] WITH(NOLOCK)\n" +
            "    WHERE TaskDefinitionId = :taskDefinitionId\n" +
            "        AND StartedAt < :olderThanDate);\n" +
            "\n" +
            "DELETE FROM [Taskling].[ForceBlockQueue]\n" +
            "WHERE ForcedDate < :olderThanDate;";
}
