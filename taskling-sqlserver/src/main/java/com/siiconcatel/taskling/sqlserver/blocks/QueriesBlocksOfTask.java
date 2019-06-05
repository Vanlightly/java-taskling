package com.siiconcatel.taskling.sqlserver.blocks;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.tasks.ReprocessOption;

import java.text.MessageFormat;

public class QueriesBlocksOfTask {
    private static final String GetBlocksOfTaskQuery =
    "SELECT B.[BlockId]\n" +
    "{0}\n" +
    "    ,BE.Attempt\n" +
    "    ,B.BlockType\n" +
    "    ,B.ObjectData\n" +
    "    ,B.CompressedObjectData\n" +
    "FROM [Taskling].[Block] B WITH(NOLOCK)\n" +
    "JOIN [Taskling].[BlockExecution] BE WITH(NOLOCK) ON B.BlockId = BE.BlockId\n" +
    "LEFT JOIN [Taskling].[TaskExecution] TE WITH(NOLOCK) ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
    "WHERE B.TaskDefinitionId = :taskDefinitionId\n" +
    "AND TE.ReferenceValue = :referenceValue\n" +
    "{1}\n" +
    "ORDER BY B.CreatedDate ASC";

    public static String getFindDateRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return MessageFormat.format(GetBlocksOfTaskQuery, ",B.FromDate,B.ToDate", "");

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return MessageFormat.format(GetBlocksOfTaskQuery, ",B.FromDate,B.ToDate", "AND BE.BlockExecutionStatus IN (0, 1, 3)");

        throw new TasklingExecutionException("ReprocessOption not supported");
    }

    public static String getFindNumericRangeBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return MessageFormat.format(GetBlocksOfTaskQuery, ",B.FromNumber,B.ToNumber", "");

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return MessageFormat.format(GetBlocksOfTaskQuery, ",B.FromNumber,B.ToNumber", "AND BE.BlockExecutionStatus IN (0, 1, 3)");

        throw new TasklingExecutionException("ReprocessOption not supported");
    }

    public static final String getFindListBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return MessageFormat.format(GetBlocksOfTaskQuery, "", "");

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return MessageFormat.format(GetBlocksOfTaskQuery, "", "AND BE.BlockExecutionStatus IN (:notStarted, :started, :failed)");

        throw new TasklingExecutionException("ReprocessOption not supported");
    }

    public static String GetFindObjectBlocksOfTaskQuery(ReprocessOption reprocessOption)
    {
        if (reprocessOption == ReprocessOption.Everything)
            return MessageFormat.format(GetBlocksOfTaskQuery, ",B.ObjectData", "");

        if (reprocessOption == ReprocessOption.PendingOrFailed)
            return MessageFormat.format(GetBlocksOfTaskQuery, ",B.ObjectData", "AND BE.BlockExecutionStatus IN (:notStarted, :started, :failed)");

        throw new TasklingExecutionException("ReprocessOption not supported");
    }
}
