package com.siiconcatel.taskling.sqlserver.blocks;

import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;

public class QueriesRangeBlock {
    public static final String InsertDateRangeBlock =
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[FromDate]\n" +
            "        ,[ToDate]\n" +
            "        ,[BlockType]\n" +
            "        ,[CreatedDate])\n" +
            "VALUES\n" +
            "        (:taskDefinitionId\n" +
            "        ,:fromDate\n" +
            "        ,:toDate\n" +
            "        ,:blockType\n" +
            "        ,GETUTCDATE());\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    public static final String InsertNumericRangeBlock =
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[FromNumber]\n" +
            "        ,[ToNumber]\n" +
            "        ,[BlockType]\n" +
            "        ,[CreatedDate])\n" +
            "VALUES\n" +
            "        (:taskDefinitionId\n" +
            "        ,:fromNumber\n" +
            "        ,:toNumber\n" +
            "        ,:blockType\n" +
            "        ,GETUTCDATE());\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    public static final String InsertBlockExecution =
            "INSERT INTO [Taskling].[BlockExecution]\n" +
            "        ([TaskExecutionId]\n" +
            "        ,[BlockId]\n" +
            "        ,[CreatedAt]\n" +
            "        ,[BlockExecutionStatus]\n" +
            "        ,[Attempt])\n" +
            "VALUES\n" +
            "        (:taskExecutionId\n" +
            "        ,:blockId\n" +
            "        ,GETUTCDATE()\n" +
            "       ,:status\n" +
            "       ,:attempt);\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    public static final String GetLastDateRangeBlockQuery =
            "SELECT TOP 1 [BlockId]\n" +
            "        ,[TaskDefinitionId]\n" +
            "        ,[FromDate]\n" +
            "        ,[ToDate]\n" +
            "        ,[FromNumber]\n" +
            "        ,[ToNumber]\n" +
            "        ,[BlockType]\n" +
            "        ,[CreatedDate]\n" +
            "FROM [Taskling].[Block]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId\n" +
            "AND IsPhantom = 0";

    public static final String GetLastNumericRangeBlockQuery =
            "SELECT TOP 1 [BlockId]\n" +
            "        ,[TaskDefinitionId]\n" +
            "        ,[FromDate]\n" +
            "        ,[ToDate]\n" +
            "        ,[FromNumber]\n" +
            "        ,[ToNumber]\n" +
            "        ,[BlockType]\n" +
            "        ,[CreatedDate]\n" +
            "FROM [Taskling].[Block]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId\n" +
            "AND IsPhantom = 0";

    public static String GetLastDateRangeBlock(LastBlockOrder lastBlockOrder)
    {
        switch (lastBlockOrder)
        {
            case LastCreated:
                return GetLastDateRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
            case RangeStart:
                return GetLastDateRangeBlockQuery + " ORDER BY [FromDate] DESC";
            case RangeEnd:
                return GetLastDateRangeBlockQuery + " ORDER BY [ToDate] DESC";
            default:
                return GetLastDateRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
        }
    }

    public static String GetLastNumericRangeBlock(LastBlockOrder lastBlockOrder)
    {
        switch (lastBlockOrder)
        {
            case LastCreated:
                return GetLastNumericRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
            case RangeStart:
                return GetLastNumericRangeBlockQuery + " ORDER BY [FromNumber] DESC";
            case RangeEnd:
                return GetLastNumericRangeBlockQuery + " ORDER BY [ToNumber] DESC";
            default:
                return GetLastNumericRangeBlockQuery + " ORDER BY [CreatedDate] DESC";
        }
    }
}
