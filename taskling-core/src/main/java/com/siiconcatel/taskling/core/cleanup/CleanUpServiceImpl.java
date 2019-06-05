package com.siiconcatel.taskling.core.cleanup;

import com.siiconcatel.taskling.core.configuration.TaskConfig;
import com.siiconcatel.taskling.core.configuration.TasklingConfiguration;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.cleanup.CleanUpRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.cleanup.CleanUpRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionCheckpointRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.utils.StringUtils;

import java.time.Duration;
import java.time.Instant;

public class CleanUpServiceImpl implements CleanUpService, Runnable {
    private final CleanUpRepository cleanUpRepository;
    private final TasklingConfiguration tasklingConfiguration;
    private final TaskExecutionRepository taskExecutionRepository;

    private String applicationName;
    private String taskName;
    private String taskExecutionId;
    private Thread t1;

    public CleanUpServiceImpl(TasklingConfiguration tasklingConfiguration,
                          CleanUpRepository cleanUpRepository,
                          TaskExecutionRepository taskExecutionRepository)
    {
        this.cleanUpRepository = cleanUpRepository;
        this.tasklingConfiguration = tasklingConfiguration;
        this.taskExecutionRepository = taskExecutionRepository;
    }

    public void cleanOldData(String applicationName, String taskName, String taskExecutionId)
    {
        this.applicationName = applicationName;
        this.taskName = taskName;
        this.taskExecutionId = taskExecutionId;

        t1 = new Thread(this);
        t1.start();
    }

    public void run()
    {
        TaskExecutionCheckpointRequest checkpoint = new TaskExecutionCheckpointRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                "");

        try
        {
            TaskConfig configuration = tasklingConfiguration.getTaskConfiguration(applicationName, taskName);
            CleanUpRequest request = new CleanUpRequest(
                    new TaskId(applicationName, taskName),
                    taskExecutionId,
                    Instant.now().minus(Duration.ofDays(configuration.getKeepGeneralDataForDays())),
                    Instant.now().minus(Duration.ofDays(configuration.getKeepListItemsForDays())),
                    Duration.ofHours(configuration.getMinimumCleanUpIntervalHours()));

            boolean cleaned = cleanUpRepository.cleanOldData(request);

            if (cleaned)
                checkpoint.setMessage("Data clean up performed");
            else
                checkpoint.setMessage("Data clean up skipped");
        }
        catch (Exception ex)
        {
            checkpoint.setMessage("Failed to clean old data. " + StringUtils.exceptionToString(ex));
        }

        logCleanup(checkpoint);
    }

    private void logCleanup(TaskExecutionCheckpointRequest checkpoint)
    {
        try
        {
            taskExecutionRepository.checkpoint(checkpoint);
        }
        catch (Exception e) { }
    }
}
