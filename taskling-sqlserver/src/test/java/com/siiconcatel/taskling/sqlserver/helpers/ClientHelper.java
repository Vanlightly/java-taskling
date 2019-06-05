package com.siiconcatel.taskling.sqlserver.helpers;

import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserver.SqlServerTasklingClient;

import java.text.MessageFormat;

public class ClientHelper {
    private static final String ConfigString =
            "\"datastoreConnection\":\"" + TestConstants.TestConnectionString + "\""+
            ",\"datastoreTimeoutSeconds\":30" +
            ",\"enabled\":true" +
            ",\"concurrency\":{0,number,#}" +
            ",\"generalRetentionDays\":{1,number,#}" +
            ",\"listItemRetentionDays\":{2,number,#}" +
            ",\"minimumCleanUpIntervalMinutes\":{3,number,#}" +
            ",\"useKeepAliveMode\":{4}" +
            ",\"taskDeathThresholdMinutes\":{5,number,#}" +
            ",\"keepAliveIntervalMinutes\":{6,number,#}" +
            ",\"reprocessFailedTasks\":{7}" +
            ",\"reprocessFailedTasksDetectionRangeMinutes\":{8,number,#}" +
            ",\"reprocessFailedTaskLimit\":{9,number,#}" +
            ",\"reprocessDeadTasks\":{10}" +
            ",\"reprocessDeadTasksDetectionRangeMinutes\":{11,number,#}" +
            ",\"reprocessDeadTaskLimit\":{12,number,#}" +
            ",\"maxBlocksToGenerate\":{13,number,#}" +
            ",\"compressBlockData\":{14}" +
            ",\"compressWhenLongerThan\":{15,number,#}" +
            ",\"itemStatusReasonMaxLength\":{16,number,#}";

    public static TaskExecutionContext getExecutionContext(String taskName, String configString)
    {
        TasklingClient client = createClient(configString);
        return client.createTaskExecutionContext(TestConstants.ApplicationName, taskName);
    }

    private static TasklingClient createClient(String configString)
    {
        return new SqlServerTasklingClient(new TestConfigurationReader(configString));
    }

    public static String getDefaultTaskConfigurationWithKeepAliveAndReprocessing(int maxBlocksToGenerate)
    {
        return "{" + MessageFormat.format(ConfigString,
                1,
                2000,
                2000,
                1,
                "true",
                10,
                1,
                "true",
                600,
                3,
                "true",
                600,
                3,
                maxBlocksToGenerate,
                "false",
                0,
                1000)
                + "}";
    }

    public static String getDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(int maxBlocksToGenerate)
    {
        return "{" + MessageFormat.format(ConfigString,
                1,
                2000,
                2000,
                1,
                "true",
                2,
                1,
                "false",
                0,
                0,
                "false",
                0,
                0,
                maxBlocksToGenerate,
                "false",
                0,
                1000)
                + "}";
    }

    public static String getDefaultTaskConfigurationWithTimePeriodOverrideAndReprocessing(int maxBlocksToGenerate)
    {
        return "{" + MessageFormat.format(ConfigString,
                1,
                2000,
                2000,
                1,
                "false",
                240,
                0,
                "true",
                600,
                3,
                "true",
                600,
                3,
                maxBlocksToGenerate,
                "false",
                0,
                1000)
                + "}";
    }

    public static String getDefaultTaskConfigurationWithTimePeriodOverrideAndNoReprocessing(int maxBlocksToGenerate)
    {
        return "{" + MessageFormat.format(ConfigString,
                1,
                2000,
                2000,
                1,
                "false",
                240,
                0,
                "false",
                0,
                0,
                "false",
                0,
                0,
                maxBlocksToGenerate,
                "false",
                0,
                1000)
                + "}";
    }
}
