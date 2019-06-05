package com.siiconcatel.taskling.sqlserver.blocks;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.TransientException;
import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.listblocks.ItemStatus;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.BatchUpdateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlockItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.SingleUpdateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.*;
import com.siiconcatel.taskling.sqlserver.blocks.serialization.SerializedValueReader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ListBlockRepositoryMsSql extends DbOperationsService implements ListBlockRepository
{
    private final TaskRepository taskRepository;

    public ListBlockRepositoryMsSql(TaskRepository taskRepository)
    {
        this.taskRepository = taskRepository;
    }

    public void changeStatus(BlockExecutionChangeStatusRequest changeStatusRequest)
    {
        try (Connection connection = createNewConnection(changeStatusRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, getListUpdateQuery(changeStatusRequest.getBlockExecutionStatus()));
            p.setLong("blockExecutionId", Long.parseLong(changeStatusRequest.getBlockExecutionId()));
            p.setInt("blockExecutionStatus", changeStatusRequest.getBlockExecutionStatus().getNumVal());
            p.executeUpdate();
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure changing block execution status", e);
        }
    }

    public List<ProtoListBlockItem> getListBlockItems(TaskId taskId, String listBlockId)
    {
        List<ProtoListBlockItem> results = new ArrayList<>();

        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesListBlock.GetListBlockItems);
            p.setLong("blockId", Long.parseLong(listBlockId));
            ResultSet rs = p.executeQuery();
            while(rs.next()) {

                ProtoListBlockItem listBlock = new ProtoListBlockItem();
                listBlock.setListBlockItemId(String.valueOf(rs.getLong("ListBlockItemId")));
                listBlock.setValue(SerializedValueReader.readValue(rs, "Value", "CompressedValue"));
                listBlock.setStatus(ItemStatus.valueOf(rs.getInt("Status")));
                listBlock.setLastUpdated(NullableField.getInstant(rs, "LastUpdated", Instant.MIN));
                listBlock.setStatusReason(NullableField.getString(rs, "StatusReason"));

                results.add(listBlock);
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure getting list block items", e);
        }



        return results;
    }

    public void updateListBlockItem(SingleUpdateRequest singeUpdateRequest)
    {
        try (Connection connection = createNewConnection(singeUpdateRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesListBlock.UpdateSingleBlockListItemStatus);
            p.setLong("blockId", Long.parseLong(singeUpdateRequest.getListBlockId()));
            p.setLong("listBlockItemId", Long.parseLong(singeUpdateRequest.getListBlockItem().getListBlockItemId()));
            p.setInt("status", singeUpdateRequest.getListBlockItem().getStatus().getNumVal());
            p.setString("statusReason", singeUpdateRequest.getListBlockItem().getStatusReason());
            p.executeUpdate();
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure updating list block item", e);
        }
    }

    public void batchUpdateListBlockItems(BatchUpdateRequest batchUpdateRequest)
    {
        try (Connection connection = createNewConnection(batchUpdateRequest.getTaskId()))
        {
            String tableName = createTemporaryTable(connection);
            SQLServerBulkListRecord dt = generateDataTable(batchUpdateRequest.getListBlockId(), batchUpdateRequest.getListBlockItems());
            bulkLoadInTransactionOperation(dt, tableName, connection);
            performBulkUpdate(connection, tableName);
            connection.commit();
        }
        catch (SQLException e) {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure performing batch list item update", e);
        }
    }

    public ProtoListBlock getLastListBlock(LastBlockRequest lastRangeBlockRequest)
    {
        TaskDefinition taskDefinition = taskRepository.ensureTaskDefinition(lastRangeBlockRequest.getTaskId());

        try (Connection connection = createNewConnection(lastRangeBlockRequest.getTaskId()))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesListBlock.GetLastListBlock);
            p.setInt("taskDefinitionId", taskDefinition.getTaskDefinitionId());
            ResultSet rs = p.executeQuery();
            if(rs.next()) {

                String listBlockId = String.valueOf(rs.getLong("BlockId"));
                ProtoListBlock listBlock = new ProtoListBlock(listBlockId, 0);
                listBlock.setItems(getListBlockItems(lastRangeBlockRequest.getTaskId(), listBlock.getListBlockId()));
                listBlock.setHeader(SerializedValueReader.readValue(rs, "ObjectData", "CompressedObjectData"));

                return listBlock;
            }
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure getting last list box", e);
        }

        return null;
    }


    private String getListUpdateQuery(BlockExecutionStatus executionStatus)
    {
        if (executionStatus == BlockExecutionStatus.Completed || executionStatus == BlockExecutionStatus.Failed)
            return QueriesBlockExecution.SetListBlockExecutionAsCompleted;

        return QueriesBlockExecution.SetBlockExecutionStatusToStarted;
    }

    private String createTemporaryTable(Connection connection) throws SQLException
    {
        String tableName = "#TempTable" + UUID.randomUUID().toString().substring(0, 10).replace('-', '0');
        connection.createStatement().execute(QueriesListBlock.getCreateTemporaryTableQuery(tableName));

        return tableName;
    }

    private SQLServerBulkListRecord generateDataTable(String listBlockId, List<ProtoListBlockItem> items)
    {
        List<Object[]> rows = new ArrayList<>();

        for (ProtoListBlockItem item : items)
        {
            Object[] dr = new Object[4];
            dr[0] = Long.parseLong(listBlockId);
            dr[1] = Long.parseLong(item.getListBlockItemId());
            dr[2] = item.getStatus().getNumVal();

            if (item.getStatusReason() == null)
                dr[3] = "";
            else
                dr[3] = item.getStatusReason();

            rows.add(dr);
        }

        SQLServerBulkListRecord bulkListRecord = new SQLServerBulkListRecord(rows);
        bulkListRecord.addColumnMetadata(1, "BlockId", Types.BIGINT, 0, 0);
        bulkListRecord.addColumnMetadata(2, "ListBlockItemId", Types.BIGINT, 0, 0);
        bulkListRecord.addColumnMetadata(3, "Status", Types.TINYINT, 0, 0);
        bulkListRecord.addColumnMetadata(4, "StatusReason", Types.NVARCHAR, -1, 0);

        return bulkListRecord;
    }

    private void performBulkUpdate(Connection connection, String tableName) throws SQLException
    {
        NamedParameterStatement p = new NamedParameterStatement(connection, QueriesListBlock.getBulkUpdateBlockListItemStatus(tableName));
        p.executeUpdate();
    }
}
