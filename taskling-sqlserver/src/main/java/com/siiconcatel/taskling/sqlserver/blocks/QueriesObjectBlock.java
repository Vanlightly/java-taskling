package com.siiconcatel.taskling.sqlserver.blocks;

public class QueriesObjectBlock {
    public static final String GetLastObjectBlock =
            "SELECT TOP 1 [BlockId]\n" +
            "        ,[TaskDefinitionId]\n" +
            "        ,[ObjectData]\n" +
            "        ,[CompressedObjectData]\n" +
            "        ,[BlockType]\n" +
            "        ,[CreatedDate]\n" +
            "FROM [Taskling].[Block]\n" +
            "WHERE [TaskDefinitionId] = :taskDefinitionId\n" +
            "AND [IsPhantom] = 0\n" +
            "ORDER BY [BlockId] DESC";

    public static final String InsertObjectBlock =
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[ObjectData]\n" +
            "        ,[CompressedObjectData]\n" +
            "        ,[BlockType]\n" +
            "        ,[CreatedDate])\n" +
            "VALUES\n" +
            "        (:taskDefinitionId\n" +
            "        ,:objectData\n" +
            "        ,:compressedObjectData\n" +
            "        ,:blockType\n" +
            "        ,GETUTCDATE());\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";
}
