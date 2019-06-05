package com.siiconcatel.taskling.core.criticalsection;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.contexts.CriticalSectionContext;
import com.siiconcatel.taskling.core.executioncontext.TaskExecutionInstance;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.*;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.GrantStatus;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.core.tasks.TaskExecutionOptions;
import com.siiconcatel.taskling.core.utils.WaitUtils;

import java.time.Duration;

public class CriticalSectionContextImpl implements CriticalSectionContext, AutoCloseable
{
    private final CriticalSectionRepository criticalSectionRepository;
    private final TaskExecutionInstance taskExecutionInstance;
    private final TaskExecutionOptions taskExecutionOptions;
    private final CriticalSectionType criticalSectionType;

    private boolean started;
    private boolean completeCalled;

    public CriticalSectionContextImpl(CriticalSectionRepository criticalSectionRepository,
        TaskExecutionInstance taskExecutionInstance,
        TaskExecutionOptions taskExecutionOptions,
        CriticalSectionType criticalSectionType)
    {
        this.criticalSectionRepository = criticalSectionRepository;
        this.taskExecutionInstance = taskExecutionInstance;
        this.taskExecutionOptions = taskExecutionOptions;
        this.criticalSectionType = criticalSectionType;

        ValidateOptions();
    }

    public boolean isActive()
    {
        return started && !completeCalled;
    }

    public boolean tryStart()
    {
        return tryStart(Duration.ofSeconds(30), 3);
    }

    public boolean tryStart(Duration retryInterval, int numberOfAttempts)
    {
        int tryCount = 0;
        boolean started = false;

        while (!started && tryCount <= numberOfAttempts)
        {
            tryCount++;
            started = tryStartCriticalSection();
            if (!started)
                WaitUtils.waitFor(retryInterval);
        }

        return started;
    }

    public void complete()
    {
        if (!isActive())
            throw new TasklingExecutionException("There is no active critical section to complete");

        CompleteCriticalSectionRequest completeRequest = new CompleteCriticalSectionRequest(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                taskExecutionInstance.getTaskExecutionId(),
                criticalSectionType);

        criticalSectionRepository.complete(completeRequest);

        completeCalled = true;
    }

    private boolean tryStartCriticalSection()
    {
        if (started)
            throw new TasklingExecutionException("There is already an active critical section");

        started = true;

        StartCriticalSectionRequest startRequest = new StartCriticalSectionRequest(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                taskExecutionInstance.getTaskExecutionId(),
                taskExecutionOptions.getTaskDeathMode(),
                criticalSectionType);

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.Override)
            startRequest.setOverrideThreshold(taskExecutionOptions.getDeathThreshold());
        else
            startRequest.setKeepAliveDeathThreshold(taskExecutionOptions.getDeathThreshold());

        StartCriticalSectionResponse response = criticalSectionRepository.start(startRequest);
        if (response.getGrantStatus() == GrantStatus.Denied)
        {
            started = false;
            return false;
        }

        return true;
    }

    private void ValidateOptions()
    {
        if (taskExecutionOptions.getDeathThreshold() == null)
            throw new TasklingExecutionException("Death threshold must be set");

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
        {
            if (!taskExecutionOptions.getKeepAliveInterval().isPresent())
                throw new TasklingExecutionException("Keep alive interval must be set when using KeepAlive mode");
        }
    }

    @Override
    public void close() throws Exception {
        if(isActive()) {
           complete();
        }
    }
}
