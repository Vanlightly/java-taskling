package com.siiconcatel.taskling.sqlserver.blocks;

import java.text.MessageFormat;

public class QueriesForcedBlockQueue {
    private static final String GetForcedBlocksQuery =
            "SELECT B.[BlockId]\n" +
            "    {0}\n" +
            "    ,COALESCE(MaxAttempt, 0) AS Attempt\n" +
            "    ,B.BlockType\n" +
            "    ,FBQ.ForceBlockQueueId\n" +
            "    ,B.ObjectData\n" +
            "    ,B.CompressedObjectData\n" +
            "FROM [Taskling].[Block] B WITH(NOLOCK)\n" +
            "JOIN [Taskling].[ForceBlockQueue] FBQ ON B.BlockId = FBQ.BlockId\n" +
            "OUTER APPLY (\n" +
            "    SELECT MAX(Attempt) MaxAttempt\n" +
            "    FROM [Taskling].[BlockExecution] WITH(NOLOCK) WHERE BlockId = FBQ.BlockId\n" +
            ") _\n" +
            "WHERE B.TaskDefinitionId = :taskDefinitionId\n" +
            "AND FBQ.ProcessingStatus = :status";

    public static String GetDateRangeBlocksQuery()
    {
        return MessageFormat.format(GetForcedBlocksQuery, ",B.FromDate,B.ToDate");
    }

    public static String GetNumericRangeBlocksQuery()
    {
        return MessageFormat.format(GetForcedBlocksQuery, ",B.FromNumber,B.ToNumber");
    }

    public static String GetListBlocksQuery()
    {
        return MessageFormat.format(GetForcedBlocksQuery, "");
    }

    public static String GetObjectBlocksQuery()
    {
        return MessageFormat.format(GetForcedBlocksQuery, ",B.ObjectData");
    }

    private static final String UpdateQuery =
            "UPDATE [Taskling].[ForceBlockQueue]\n" +
            "SET [ProcessingStatus] = ''Execution Created''\n" +
            "WHERE ForceBlockQueueId IN ({0})";

    public static String GetUpdateQuery(int blockCount)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < blockCount; i++)
        {
            if (i > 0)
                sb.append(",");

            sb.append(":p" + i);
        }

        return MessageFormat.format(UpdateQuery, sb.toString());
    }
}
