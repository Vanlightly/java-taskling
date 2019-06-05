package com.siiconcatel.taskling.core;

import java.time.Duration;

public class ClientConnectionSettings
{
    public ClientConnectionSettings(String connectionString, Duration queryTimeout)
    {
        this.connectionString = connectionString;
        this.queryTimeout = queryTimeout;
    }

    private String connectionString;
    private Duration queryTimeout;

    public Duration getQueryTimeout()
    {
        return queryTimeout;
    }

    public String getConnectionString() {
        return connectionString;
    }
}
