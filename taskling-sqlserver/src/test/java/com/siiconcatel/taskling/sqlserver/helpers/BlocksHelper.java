package com.siiconcatel.taskling.sqlserver.helpers;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.ItemStatus;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockItem;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockItemImpl;
import com.siiconcatel.taskling.core.serde.TasklingSerde;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NullableField;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;
import com.siiconcatel.taskling.sqlserver.blocks.serialization.SerializedValueReader;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BlocksHelper {

    private static final String InsertDateRangeBlockQuery =
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[FromDate]\n" +
            "        ,[ToDate]\n" +
            "        ,[CreatedDate]\n" +
            "        ,[BlockType])\n" +
            "VALUES\n" +
            "        (:taskDefinitionId\n" +
            "         ,:fromDate\n" +
            "         ,:toDate\n" +
            "         ,:createdDate\n" +
            "         ,:blockType);\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private static final String InsertNumericRangeBlockQuery =
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[FromNumber]\n" +
            "        ,[ToNumber]\n" +
            "        ,[CreatedDate]\n" +
            "        ,[BlockType])\n" +
            "VALUES\n" +
            "        (:taskDefinitionId\n" +
            "         ,:fromNumber\n" +
            "         ,:toNumber\n" +
            "         ,:createdDate\n" +
            "         ,:blockType);\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private static final String InsertListBlockQuery =
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[CreatedDate]\n" +
            "        ,[BlockType]\n" +
            "        ,[ObjectData])\n" +
            "VALUES\n" +
            "        (:taskDefinitionId\n" +
            "         ,:createdDate\n" +
            "         ,:blockType\n" +
            "         ,:objectData);\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private static final String InsertObjectBlockQuery =
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[CreatedDate]\n" +
            "        ,[BlockType]\n" +
            "        ,[ObjectData])\n" +
            "VALUES\n" +
            "        (:taskDefinitionId\n" +
            "         ,:createdDate\n" +
            "         ,:blockType\n" +
            "         ,:objectData);\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private static final String InsertBlockExecutionQuery =
            "INSERT INTO [Taskling].[BlockExecution]\n" +
            "        ([TaskExecutionId]\n" +
            "        ,[BlockId]\n" +
            "        ,[CreatedAt]\n" +
            "        ,[StartedAt]\n" +
            "        ,[CompletedAt]\n" +
            "        ,[BlockExecutionStatus]\n" +
            "        ,[Attempt])\n" +
            "VALUES\n" +
            "        (:taskExecutionId\n" +
            "         ,:blockId\n" +
            "         ,:createdAt\n" +
            "         ,:startedAt\n" +
            "         ,:completedAt\n" +
            "         ,:blockExecutionStatus\n" +
            "         ,:attempt);\n" +
            "\n" +
            "SELECT CAST(SCOPE_IDENTITY() AS BIGINT);";

    private static final String DeleteBlocksQuery =
            "DELETE BE FROM [Taskling].[BlockExecution] BE\n" +
            "LEFT JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
            "LEFT JOIN [Taskling].[TaskDefinition] T ON TE.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE (T.ApplicationName = :applicationName)\n" +
            "OR T.TaskDefinitionId IS NULL\n" +
            "OR TE.TaskExecutionId IS NULL;\n" +
            "\n" +
            "DELETE B FROM [Taskling].[Block] B\n" +
            "LEFT JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE (T.ApplicationName = :applicationName)\n" +
            "OR T.TaskDefinitionId IS NULL;\n" +
            "\n" +
            "DELETE LBI FROM [Taskling].[ListBlockItem] LBI\n" +
            "LEFT JOIN [Taskling].[Block] B ON LBI.BlockId = B.BlockId\n" +
            "LEFT JOIN [Taskling].[TaskDefinition] T ON B.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE (T.ApplicationName = :applicationName)\n" +
            "OR T.TaskDefinitionId IS NULL\n" +
            "OR B.BlockId IS NULL;";

    private static final String GetBlockCountQuery =
            "SELECT COUNT(*)\n" +
            "FROM [Taskling].[Block] B\n" +
            "JOIN [Taskling].[TaskDefinition]  T ON B.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE T.ApplicationName = :applicationName\n" +
            "AND T.TaskName = :taskName;";

    private static final String GetBlockExecutionsCountByStatusQuery =
            "SELECT COUNT(*)\n" +
            "FROM [Taskling].[BlockExecution] BE\n" +
            "JOIN [Taskling].[TaskExecution] TE ON BE.TaskExecutionId = TE.TaskExecutionId\n" +
            "JOIN [Taskling].[TaskDefinition]  T ON TE.TaskDefinitionId = T.TaskDefinitionId\n" +
            "WHERE T.ApplicationName = :applicationName\n" +
            "AND T.TaskName = :taskName\n" +
            "AND BE.BlockExecutionStatus = :blockExecutionStatus;";

    private static final String GetListBlockItemCountByStatusQuery =
            "SELECT COUNT(*)\n" +
            "FROM [Taskling].[ListBlockItem] LBI\n" +
            "WHERE LBI.BlockId = :blockId\n" +
            "AND LBI.Status = :status;";

    private static final String GetItemsCountQuery =
            "SELECT [ItemsCount]\n" +
            "FROM [Taskling].[BlockExecution]\n" +
            "WHERE [BlockExecutionId] = :blockExecutionId";

    private static final String GetLastBlockIdQuery =
            "SELECT MAX(BlockId)\n" +
            "FROM [Taskling].[Block] B\n" +
            "JOIN [Taskling].[TaskDefinition] TD ON B.TaskDefinitionId = TD.TaskDefinitionId\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "AND TaskName = :taskName";

    private static final String GetListBlockItemsQuery =
            "SELECT [ListBlockItemId]\n" +
            "        ,[Value]\n" +
            "        ,[CompressedValue]\n" +
            "        ,[Status]\n" +
            "        ,[LastUpdated]\n" +
            "        ,[StatusReason]\n" +
            "FROM [Taskling].[ListBlockItem]\n" +
            "WHERE [BlockId] = :blockId\n" +
            "AND [Status] = :status";

    private static final String InsertForcedBlockQueueQuery =
            "INSERT INTO [Taskling].[ForceBlockQueue]\n" +
            "         ([BlockId]\n" +
            "         ,[ForcedBy]\n" +
            "         ,[ForcedDate]\n" +
            "         ,[ProcessingStatus])\n" +
            "VALUES\n" +
            "         (:blockId\n" +
            "         ,'Test'\n" +
            "         ,GETUTCDATE()\n" +
            "         ,'Pending')";

    private static final String InsertPhantomNumericBlockQuery =
            "DECLARE @taskDefinitionId INT = (\n" +
            "SELECT TaskDefinitionId\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "AND TaskName = :taskName)\n" +
            "\n" +
            "INSERT INTO [Taskling].[Block]\n" +
            "         ([TaskDefinitionId]\n" +
            "         ,[FromNumber]\n" +
            "         ,[ToNumber]\n" +
            "         ,[BlockType]\n" +
            "         ,[IsPhantom]\n" +
            "         ,[CreatedDate])\n" +
            "VALUES\n" +
            "         (@taskDefinitionId\n" +
            "         ,:fromNumber\n" +
            "         ,:toNumber\n" +
            "         ,:blockType\n" +
            "         ,1\n" +
            "        ,GETUTCDATE())";

    private static final String InsertPhantomDateBlockQuery =
            "DECLARE @taskDefinitionId INT = (\n" +
            "SELECT TaskDefinitionId\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "AND TaskName = :taskName)\n" +
            "\n" +
            "INSERT INTO [Taskling].[Block]\n" +
            "        ([TaskDefinitionId]\n" +
            "        ,[FromDate]\n" +
            "        ,[ToDate]\n" +
            "        ,[BlockType]\n" +
            "        ,[IsPhantom]\n" +
            "        ,[CreatedDate])\n" +
            "VALUES\n" +
            "        (@taskDefinitionId\n" +
            "        ,:fromDate\n" +
            "        ,:toDate\n" +
            "        ,:blockType\n" +
            "        ,1\n" +
            "        ,GETUTCDATE())";

    private static final String InsertPhantomListBlockQuery =
            "DECLARE @taskDefinitionId INT = (\n" +
            "SELECT TaskDefinitionId\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "AND TaskName = :taskName)\n" +
            "\n" +
            "INSERT INTO [Taskling].[Block]\n" +
            "         ([TaskDefinitionId]\n" +
            "         ,[BlockType]\n" +
            "         ,[IsPhantom]\n" +
            "         ,[CreatedDate])\n" +
            "VALUES\n" +
            "         (@taskDefinitionId\n" +
            "         ,:blockType\n" +
            "         ,1\n" +
            "         ,GETUTCDATE())\n" +
            "\n" +
            "DECLARE @blockId BIGINT = (SELECT CAST(SCOPE_IDENTITY() AS BIGINT))\n" +
            "\n" +
            "INSERT INTO [Taskling].[ListBlockItem]\n" +
            "         ([BlockId]\n" +
            "         ,[Value]\n" +
            "         ,[Status])\n" +
            "VALUES\n" +
            "        (@blockId\n" +
            "        ,'test'\n" +
            "        ,1)";

    private static final String InsertPhantomObjectBlockQuery =
            "DECLARE @taskDefinitionId INT = (\n" +
            "SELECT TaskDefinitionId\n" +
            "FROM [Taskling].[TaskDefinition]\n" +
            "WHERE ApplicationName = :applicationName\n" +
            "         AND TaskName = :taskName)\n" +
            "\n" +
            "INSERT INTO [Taskling].[Block]\n" +
            "         ([TaskDefinitionId]\n" +
            "         ,[ObjectData]\n" +
            "         ,[BlockType]\n" +
            "         ,[IsPhantom]\n" +
            "         ,[CreatedDate])\n" +
            "VALUES\n" +
            "         (@taskDefinitionId\n" +
            "         ,:objectData\n" +
            "         ,:blockType\n" +
            "         ,1\n" +
            "         ,GETUTCDATE())";



    public long insertDateRangeBlock(int taskDefinitionId, Instant fromDate, Instant toDate)
    {
        return insertDateRangeBlock(taskDefinitionId, fromDate, toDate, fromDate);
    }

    public long insertDateRangeBlock(int taskDefinitionId,
                                     Instant fromDate,
                                     Instant toDate,
                                     Instant createdAt)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertDateRangeBlockQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setTimestamp("fromDate", TimeHelper.toTimestamp(fromDate));
            p.setTimestamp("toDate", TimeHelper.toTimestamp(toDate));
            p.setTimestamp("createdDate", TimeHelper.toTimestamp(createdAt));
            p.setInt("blockType", BlockType.DateRange.getNumVal());

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getLong(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public long insertNumericRangeBlock(int taskDefinitionId,
                                        long fromNumber,
                                        long toNumber,
                                        Instant createdDate)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertNumericRangeBlockQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setLong("fromNumber", fromNumber);
            p.setLong("toNumber", toNumber);
            p.setTimestamp("createdDate", TimeHelper.toTimestamp(createdDate));
            p.setInt("blockType", BlockType.NumericRange.getNumVal());

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getLong(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public long insertListBlock(int taskDefinitionId, Instant createdDate, String objectData)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertListBlockQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setTimestamp("createdDate", TimeHelper.toTimestamp(createdDate));
            p.setInt("blockType", BlockType.List.getNumVal());
            p.setString("objectData", objectData);

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getLong(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public long insertObjectBlock(int taskDefinitionId, Instant createdDate, Object objectData)
    {
        String jsonData = TasklingSerde.serialize(objectData, false);
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertObjectBlockQuery);
            p.setInt("taskDefinitionId", taskDefinitionId);
            p.setTimestamp("createdDate", TimeHelper.toTimestamp(createdDate));
            p.setInt("blockType", BlockType.Object.getNumVal());
            p.setString("objectData", jsonData);

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getLong(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public long insertBlockExecution(String taskExecutionId,
                                     long blockId,
                                     Instant createdAt,
                                     Instant startedAt,
                                     Instant completedAt,
                                     BlockExecutionStatus executionStatus,
                                     int attempt)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertBlockExecutionQuery);
            p.setInt("taskExecutionId", Integer.parseInt(taskExecutionId));
            p.setLong("blockId", blockId);
            p.setTimestamp("createdAt", TimeHelper.toTimestamp(createdAt));
            p.setLong("attempt", attempt);
            p.setTimestamp("startedAt", startedAt == null ? null : TimeHelper.toTimestamp(startedAt));
            p.setTimestamp("completedAt", completedAt == null ? null : TimeHelper.toTimestamp(completedAt));
            p.setInt("blockExecutionStatus", executionStatus.getNumVal());

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getLong(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void deleteBlocks(String applicationName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, DeleteBlocksQuery);
            p.setString("applicationName", applicationName);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public int getBlockCount(String applicationName, String taskName)
    {
        return getBlockCount(applicationName, taskName, GetBlockCountQuery);
    }

    private int getBlockCount(String applicationName, String taskName, String query)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getInt(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public int getBlockExecutionCountByStatus(String applicationName,
                                              String taskName,
                                              BlockExecutionStatus blockExecutionStatus)
    {
        return getBlockExecutionCountByStatus(applicationName, taskName, blockExecutionStatus, GetBlockExecutionsCountByStatusQuery);
    }

    private int getBlockExecutionCountByStatus(String applicationName,
                                               String taskName,
                                               BlockExecutionStatus blockExecutionStatus,
                                               String query)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, query);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            p.setInt("blockExecutionStatus", blockExecutionStatus.getNumVal());

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getInt(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public int getBlockExecutionItemCount(long blockExecutionId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetItemsCountQuery);
            p.setLong("blockExecutionId", blockExecutionId);

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getInt(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public int getListBlockItemCountByStatus(String blockId, ItemStatus status)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetListBlockItemCountByStatusQuery);
            p.setLong("blockId", Long.parseLong(blockId));
            p.setInt("status", status.getNumVal());

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getInt(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public long getLastBlockId(String applicationName, String taskName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetLastBlockIdQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);

            ResultSet rs = p.executeQuery();
            rs.next();

            return rs.getLong(1);
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public <T> List<ListBlockItem<T>> getListBlockItems(Class<T> itemType, String blockId, ItemStatus status)
    {
        List<ListBlockItem<T>> items = new ArrayList<>();

        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, GetListBlockItemsQuery);
            p.setLong("blockId", Long.parseLong(blockId));
            p.setInt("status", status.getNumVal());

            ResultSet rs = p.executeQuery();
            while(rs.next()) {


                ListBlockItem<T> item = new ListBlockItemImpl<T>(
                        String.valueOf(rs.getLong(1)),
                        TasklingSerde.deserialize(itemType, SerializedValueReader.readValue(rs, "Value", "CompressedValue"), false),
                        ItemStatus.valueOf(rs.getInt(4)),
                        NullableField.getString(rs, "StatusReason"),
                        NullableField.getInstant(rs, "LastUpdated"));

                items.add(item);
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }

        return items;
    }

    public void enqueueForcedBlock(long blockId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertForcedBlockQueueQuery);
            p.setLong("blockId", blockId);
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void insertPhantomDateRangeBlock(String applicationName,
                                            String taskName,
                                            Instant fromDate,
                                            Instant toDate)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertPhantomDateBlockQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            p.setTimestamp("fromDate", TimeHelper.toTimestamp(fromDate));
            p.setTimestamp("toDate", TimeHelper.toTimestamp(toDate));
            p.setInt("blockType", BlockType.DateRange.getNumVal());

            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void insertPhantomNumericBlock(String applicationName, String taskName, long fromId, long toId)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertPhantomNumericBlockQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            p.setLong("fromNumber", fromId);
            p.setLong("toNumber", toId);
            p.setInt("blockType", BlockType.NumericRange.getNumVal());

            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void insertPhantomListBlock(String applicationName, String taskName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertPhantomListBlockQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            p.setInt("blockType", BlockType.List.getNumVal());

            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    public void insertPhantomObjectBlock(String applicationName, String taskName)
    {
        try (Connection connection = createNewConnection()) {
            NamedParameterStatement p= new NamedParameterStatement(connection, InsertPhantomObjectBlockQuery);
            p.setString("applicationName", applicationName);
            p.setString("taskName", taskName);
            p.setString("objectData", TasklingSerde.serialize("My phantom block", false));
            p.setInt("blockType", BlockType.List.getNumVal());

            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create date range block", e);
        }
    }

    protected Connection createNewConnection() throws SQLException
    {
        return DriverManager.getConnection(TestConstants.TestConnectionString);
    }
}
