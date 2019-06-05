package com.siiconcatel.taskling.sqlserver.blocks;

import java.text.MessageFormat;

public class QueriesDeadBlocks {
    private static final String FindDeadBlocksQuery =
            "WITH OrderedBlocks As (\n" +
            "    SELECT ROW_NUMBER() OVER (PARTITION BY BE.BlockId ORDER BY BE.BlockExecutionId DESC) AS RowNo\n" +
            "            ,BE.[BlockExecutionId]\n" +
            "    FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)\n" +
            "    JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
            "    WHERE TE.TaskDefinitionId = :taskDefinitionId\n" +
            "    AND TE.StartedAt >= :searchPeriodBegin\n" +
            "    AND TE.StartedAt < :searchPeriodEnd\n" +
            ")\n" +
            "\n" +
            "SELECT TOP {0,number,#} B.[BlockId]\n" +
            "    {1}\n" +
            "    ,BE.Attempt\n" +
            "    ,B.BlockType\n" +
            "    ,B.ObjectData\n" +
            "    ,B.CompressedObjectData\n" +
            "FROM [Taskling].[Block] B WITH(NOLOCK)\n" +
            "JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId\n" +
            "JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
            "JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId\n" +
            "WHERE B.TaskDefinitionId = :taskDefinitionId\n" +
            "AND B.IsPhantom = 0\n" +
            "AND TE.StartedAt <= DATEADD(SECOND, -1 * DATEDIFF(SECOND, ''00:00:00'', OverrideThreshold), GETUTCDATE())\n" +
            "AND BE.BlockExecutionStatus IN (1,2)\n" +
            "AND BE.Attempt < :attemptLimit\n" +
            "AND OB.RowNo = 1\n" +
            "ORDER BY B.CreatedDate ASC";

    private static final String FindDeadBlocksWithKeepAliveQuery =
            "WITH OrderedBlocks As (\n" +
            "    SELECT ROW_NUMBER() OVER (PARTITION BY BE.BlockId ORDER BY BE.BlockExecutionId DESC) AS RowNo\n" +
            "            ,BE.[BlockExecutionId]\n" +
            "    FROM [Taskling].[BlockExecution] BE WITH(NOLOCK)\n" +
            "    JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
            "    WHERE TE.TaskDefinitionId = :taskDefinitionId\n" +
            "    AND TE.StartedAt  >= :searchPeriodBegin\n" +
            "    AND TE.StartedAt < :searchPeriodEnd\n" +
            ")\n" +
            "\n" +
            "SELECT TOP {0,number,#} B.[BlockId]\n" +
            "    {1}\n" +
            "    ,BE.Attempt\n" +
            "    ,B.BlockType\n" +
            "    ,B.ObjectData\n" +
            "    ,B.CompressedObjectData\n" +
            "FROM [Taskling].[Block] B WITH(NOLOCK)\n" +
            "JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId\n" +
            "JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
            "JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId\n" +
            "WHERE B.TaskDefinitionId = :taskDefinitionId\n" +
            "AND B.IsPhantom = 0\n" +
            "AND DATEDIFF(SECOND, TE.LastKeepAlive, GETUTCDATE()) > DATEDIFF(SECOND, ''00:00:00'', TE.KeepAliveDeathThreshold)\n" +
            "AND BE.BlockExecutionStatus IN (1,2)\n" +
            "AND BE.Attempt < :attemptLimit\n" +
            "AND OB.RowNo = 1\n" +
            "ORDER BY B.CreatedDate ASC";

    public static String getFindDeadDateRangeBlocksQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksQuery, top, ",B.FromDate,B.ToDate");
    }

    public static String getFindDeadNumericRangeBlocksQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksQuery, top, ",B.FromNumber,B.ToNumber");
    }

    public static String getFindDeadListBlocksQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksQuery, top, "");
    }

    public static String getFindDeadObjectBlocksQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksQuery, top, ",B.ObjectData");
    }

    public static String getFindDeadDateRangeBlocksWithKeepAliveQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksWithKeepAliveQuery, top, ",B.FromDate,B.ToDate");
    }

    public static String getFindDeadNumericRangeBlocksWithKeepAliveQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksWithKeepAliveQuery, top, ",B.FromNumber,B.ToNumber");
    }

    public static String getFindDeadListBlocksWithKeepAliveQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksWithKeepAliveQuery, top, "");
    }

    public static String getFindDeadObjectBlocksWithKeepAliveQuery(int top)
    {
        return MessageFormat.format(FindDeadBlocksWithKeepAliveQuery, top, ",B.ObjectData");
    }
}
