package com.siiconcatel.taskling.sqlserver.blocks;

import java.text.MessageFormat;

public class QueriesFailedBlocks {
    private static final String FindFailedBlocksQuery =
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
                "    ,TE.ReferenceValue\n" +
                "    ,B.ObjectData\n" +
                "    ,B.CompressedObjectData\n" +
                "FROM [Taskling].[Block] B WITH(NOLOCK)\n" +
                "JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId\n" +
                "JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
                "JOIN OrderedBlocks OB ON BE.BlockExecutionId = OB.BlockExecutionId\n" +
                "WHERE B.TaskDefinitionId = :taskDefinitionId\n" +
                "AND B.IsPhantom = 0\n" +
                "AND BE.BlockExecutionStatus = 4\n" +
                "AND BE.Attempt < :attemptLimit\n" +
                "AND OB.RowNo = 1\n" +
                "ORDER BY B.CreatedDate ASC";

    public static String getFindFailedDateRangeBlocksQuery(int top)
    {
        return MessageFormat.format(FindFailedBlocksQuery, top, ",B.FromDate,B.ToDate");
    }

    public static String GetFindFailedNumericRangeBlocksQuery(int top)
    {
        return MessageFormat.format(FindFailedBlocksQuery, top, ",B.FromNumber,B.ToNumber");
    }

    public static String GetFindFailedListBlocksQuery(int top)
    {
        return MessageFormat.format(FindFailedBlocksQuery, top, "");
    }

    public static String GetFindFailedObjectBlocksQuery(int top)
    {
        return MessageFormat.format(FindFailedBlocksQuery, top, ",B.ObjectData");
    }
}
