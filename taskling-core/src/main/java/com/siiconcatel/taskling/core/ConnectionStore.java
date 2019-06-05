package com.siiconcatel.taskling.core;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConnectionStore
{
    private static final ConnectionStore instance = new ConnectionStore();
    private static ReadWriteLock mapLock = new ReentrantReadWriteLock();
    private static HashMap<TaskId, ClientConnectionSettings> connections;

    private ConnectionStore()
    {
        connections = new HashMap<TaskId, ClientConnectionSettings>();
    }

    public static ConnectionStore getInstance()
    {
        return instance;
    }

    public void setConnection(TaskId taskId, ClientConnectionSettings connectionSettings)
    {
        mapLock.writeLock().lock();
        try
        {
            connections.put(taskId, connectionSettings);
        }
        finally {
            mapLock.writeLock().unlock();
        }
    }

    public ClientConnectionSettings getConnection(TaskId taskId)
    {
        mapLock.readLock().lock();
        try
        {
            if (connections.containsKey(taskId))
                return connections.get(taskId);
            return null;
        }
        finally {
            mapLock.readLock().unlock();
        }
    }
}
