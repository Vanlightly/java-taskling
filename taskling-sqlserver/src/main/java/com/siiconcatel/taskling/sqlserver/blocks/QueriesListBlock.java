package com.siiconcatel.taskling.sqlserver.blocks;

import java.text.MessageFormat;

public class QueriesListBlock {
    public static final String InsertListBlock =
                "INSERT INTO [Taskling].[Block]\n" +
                "    ([TaskDefinitionId]\n" +
                "    ,[BlockType]\n" +
                "    ,[CreatedDate]\n" +
                "    ,[ObjectData]\n" +
                "    ,[CompressedObjectData])\n" +
                "VALUES\n" +
                "   (:taskDefinitionId\n" +
                "    ,:blockType\n" +
                "    ,GETUTCDATE()\n" +
                "    ,:objectData\n" +
                "    ,:compressedObjectData);\n" +
                "\n" +
                "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    public static final String GetListBlockItems =
            "SELECT [ListBlockItemId]\n" +
            "        ,[BlockId]\n" +
            "        ,[Value]\n" +
            "        ,[CompressedValue]\n" +
            "        ,[Status]\n" +
            "        ,[LastUpdated]\n" +
            "        ,[StatusReason]\n" +
            "FROM [Taskling].[ListBlockItem]\n" +
            "WHERE [BlockId] = :blockId";

    public static final String UpdateSingleBlockListItemStatus =
            "UPDATE [Taskling].[ListBlockItem]\n" +
            "SET [Status] = :status\n" +
            "    ,[StatusReason] = :statusReason\n" +
            "    ,[LastUpdated] = GETUTCDATE()\n" +
            "WHERE BlockId = :blockId\n" +
            "AND ListBlockItemId = :listBlockItemId";

    private static final String CreateTemporaryTable =
            "CREATE TABLE {0}(\n" +
            "    [ListBlockItemId] bigint NOT NULL,\n" +
            "    [BlockId] bigint NOT NULL,\n" +
            "    [Status] tinyint NOT NULL,\n" +
            "    [StatusReason] nvarchar(max) NULL,\n" +
            ");";

    private static final String BulkUpdateBlockListItemStatus =
            "UPDATE LBI\n" +
            "SET [Status] = T.[Status]\n" +
            "        ,[StatusReason] = T.[StatusReason]\n" +
            "        ,[LastUpdated] = GETUTCDATE()\n" +
            "FROM [Taskling].[ListBlockItem] LBI\n" +
            "JOIN {0} AS T ON LBI.BlockId = T.BlockId\n" +
            "AND LBI.ListBlockItemId = T.ListBlockItemId";

    public static final String GetLastListBlock =
            "SELECT TOP 1 [BlockId]\n" +
            "        ,[TaskDefinitionId]\n" +
            "        ,[FromDate]\n" +
            "        ,[ToDate]\n" +
            "        ,[FromNumber]\n" +
            "        ,[ToNumber]\n" +
            "        ,[BlockType]\n" +
            "        ,[CreatedDate]\n" +
            "        ,[ObjectData]\n" +
            "        ,[CompressedObjectData]\n" +
            "FROM [Taskling].[Block]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId\n" +
            "AND [IsPhantom] = 0\n" +
            "ORDER BY [BlockId] DESC";

    public static String getCreateTemporaryTableQuery(String tableName)
    {
        return MessageFormat.format(CreateTemporaryTable, tableName);
    }

    public static String getBulkUpdateBlockListItemStatus(String tableName)
    {
        return MessageFormat.format(BulkUpdateBlockListItemStatus, tableName);
    }
}
