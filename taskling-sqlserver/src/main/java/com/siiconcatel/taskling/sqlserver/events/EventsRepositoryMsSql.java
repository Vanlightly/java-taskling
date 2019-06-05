package com.siiconcatel.taskling.sqlserver.events;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.TransientException;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TransientErrorDetector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

public class EventsRepositoryMsSql extends DbOperationsService implements EventsRepository {

    public void logEvent(TaskId taskId, String taskExecutionId, EventType eventType, String message)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTaskExecutionEvent.InsertTaskExecutionEventQuery);
            p.setInt("taskExecutionId", Integer.parseInt(taskExecutionId));
            p.setInt("eventType", eventType.getNumVal());
            p.setString("message", message);
            p.setTimestamp("eventDateTime", TimeHelper.toTimestamp(Instant.now()));
            p.executeUpdate();
        }
        catch (SQLException e)
        {
            if (TransientErrorDetector.isTransient(e))
                throw new TransientException("A transient exception has occurred", e);

            throw new TasklingExecutionException("Failure adding a new block execution", e);
        }
    }
}
