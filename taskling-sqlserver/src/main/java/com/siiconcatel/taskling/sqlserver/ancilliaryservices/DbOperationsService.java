package com.siiconcatel.taskling.sqlserver.ancilliaryservices;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.siiconcatel.taskling.core.ConnectionStore;
import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.utils.WaitUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbOperationsService {
    protected Connection createNewConnection(TaskId taskId)
    {
        int attempts = 0;

        while(attempts <= 3) {
            try {
                attempts++;
                Connection connection = DriverManager.getConnection(ConnectionStore.getInstance().getConnection(taskId).getConnectionString());

                return connection;
            } catch (SQLException sqlEx) {
                if (TransientErrorDetector.isTransient(sqlEx))
                {
                    if(attempts <= 3)
                        WaitUtils.waitForMs(1000);
                    else
                        throw new TasklingExecutionException("Couldn't connect to the databse", sqlEx);
                }
            }
        }

        return null;
    }

    protected String limitLength(String inputStr, int lengthLimit)
    {
        if (inputStr == null)
            return inputStr;

        if (inputStr.length() > lengthLimit)
            return inputStr.substring(0, lengthLimit);

        return inputStr;
    }

    protected void bulkLoadInTransactionOperation(SQLServerBulkListRecord listRecord, String tableNameAndSchema, Connection connection) throws SQLException
    {
        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        connection.setAutoCommit(false);
        SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
        copyOptions.setBulkCopyTimeout(360);

        for (String column : listRecord.getColumnsNames())
            bulkCopy.addColumnMapping(column, column);

        bulkCopy.setBulkCopyOptions(copyOptions);
        bulkCopy.setDestinationTableName(tableNameAndSchema);
        bulkCopy.writeToServer(listRecord);
    }
}
