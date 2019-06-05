package com.siiconcatel.taskling.sqlserver.helpers;

import java.time.Duration;

public class TestConstants {
    public static final String TestConnectionString = "jdbc:sqlserver://localhost;databaseName=TasklingDb;integratedSecurity=true;allowMultiQueries=true";
    public static final String ApplicationName = "MyTestApplication";
    public static final String TaskName = "MyTestTask";
    public static final Duration QueryTimeout = Duration.ofMinutes(1);
}
