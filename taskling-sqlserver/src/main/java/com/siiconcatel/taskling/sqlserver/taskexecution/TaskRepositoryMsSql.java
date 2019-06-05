package com.siiconcatel.taskling.sqlserver.taskexecution;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.core.utils.WaitUtils;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.TimeHelper;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TaskRepositoryMsSql extends DbOperationsService implements TaskRepository {
    private static Lock cacheSemaphore = new ReentrantLock();
    private static Lock getTaskSemaphore = new ReentrantLock();
    private static HashMap<TaskId, CachedTaskDefinition> cachedTaskDefinitions = new HashMap<TaskId, CachedTaskDefinition>();

    public TaskDefinition ensureTaskDefinition(TaskId taskId)
    {
        getTaskSemaphore.lock();
        try
        {
            TaskDefinition taskDefinition = getTask(taskId);
            if (taskDefinition != null)
            {
                return taskDefinition;
            }
            else
            {
                // wait a random amount of time in case two threads or two instances of this repository
                // independently belive that the task doesn't exist
                WaitUtils.waitForRandomMs(2000);
                taskDefinition = taskDefinition = getTask(taskId);;
                if (taskDefinition != null)
                {
                    return taskDefinition;
                }

                return insertNewTask(taskId);
            }
        }
        finally
        {
            getTaskSemaphore.unlock();
        }
    }

    public Instant getLastTaskCleanUpTime(TaskId taskId)
    {
        try (Connection connection = createNewConnection(taskId)) {
            NamedParameterStatement p = new NamedParameterStatement(connection, QueriesTask.GetLastCleanUpTimeQuery);
            p.setString("applicationName", taskId.getApplicationName());
            p.setString("taskName", taskId.getTaskName());
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                Timestamp lastCleaned = rs.getTimestamp(1);
                return TimeHelper.toInstant(lastCleaned);
            }
            else {
                return Instant.MIN;
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to retrieve task definition", e);
        }
    }

    public void setLastCleaned(TaskId taskId)
    {
        try (Connection connection = createNewConnection(taskId)) {
            NamedParameterStatement p = new NamedParameterStatement(connection, QueriesTask.SetLastCleanUpTimeQuery);
            p.setString("applicationName", taskId.getApplicationName());
            p.setString("taskName", taskId.getTaskName());
            p.execute();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to retrieve task definition", e);
        }
    }

    public static void clearCache()
    {
        cacheSemaphore.lock();
        try
        {
            cachedTaskDefinitions.clear();
        }
        finally
        {
            cacheSemaphore.unlock();
        }
    }

    private TaskDefinition getTask(TaskId taskId)
    {
        cacheSemaphore.lock();
        try
        {
            TaskDefinition taskDefinition = null;
            if (cachedTaskDefinitions.containsKey(taskId))
            {
                CachedTaskDefinition definition = cachedTaskDefinitions.get(taskId);
                if (Duration.between(Instant.now(), definition.getCachedAt()).getSeconds() < 300)
                    taskDefinition = definition.getTaskDefinition();
            }


            if(taskDefinition == null) {
                taskDefinition = loadTask(taskId);
                if(taskDefinition == null) {
                    return null;
                }
                else {
                    cacheTaskDefinition(taskId, taskDefinition);
                    return taskDefinition;
                }
            }
            else {
                return taskDefinition;
            }
        }
        finally
        {
            cacheSemaphore.unlock();
        }
    }

    private void cacheTaskDefinition(TaskId taskKey, TaskDefinition taskDefinition)
    {
        cachedTaskDefinitions.put(taskKey, new CachedTaskDefinition(taskDefinition));
    }

    private TaskDefinition loadTask(TaskId taskId)
    {
        try (Connection connection = createNewConnection(taskId)) {
            NamedParameterStatement p = new NamedParameterStatement(connection, QueriesTask.GetTaskQuery);
            p.setString("applicationName", taskId.getApplicationName());
            p.setString("taskName", taskId.getTaskName());
            ResultSet rs = p.executeQuery();
            if(rs.next()) {
                int id = rs.getInt(3);
                TaskDefinition taskDefinition = new TaskDefinition();
                taskDefinition.setTaskDefinitionId(id);
                taskDefinition.setApplicationName(taskId.getApplicationName());
                taskDefinition.setTaskName(taskId.getTaskName());
                return taskDefinition;
            }
            else {
                return null;
            }
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to retrieve task definition", e);
        }
    }

    private TaskDefinition insertNewTask(TaskId taskId)
    {
        try (Connection connection = createNewConnection(taskId))
        {
            NamedParameterStatement p= new NamedParameterStatement(connection, QueriesTask.InsertTaskQuery);
            p.setString("applicationName", taskId.getApplicationName());
            p.setString("taskName", taskId.getTaskName());
            ResultSet rs = p.executeQuery();
            rs.next();
            int id = rs.getInt(1);

            TaskDefinition taskDefinition = new TaskDefinition();
            taskDefinition.setTaskDefinitionId(id);
            taskDefinition.setApplicationName(taskId.getApplicationName());
            taskDefinition.setTaskName(taskId.getTaskName());

            cacheSemaphore.lock();
            try
            {
                cacheTaskDefinition(taskId, taskDefinition);
            }
            finally
            {
                cacheSemaphore.unlock();
            }

            return taskDefinition;
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to create task definition", e);
        }
    }
}
