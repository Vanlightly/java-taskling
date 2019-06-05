package com.siiconcatel.taskling.sqlserver.contexts.executions;

import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.categories.TaskExecutionTests;
import com.siiconcatel.taskling.sqlserver.helpers.ClientHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenStatus;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class When_TryWithResources {
    public When_TryWithResources() {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_InUsingBlockAndNoExecutionTokenExists_ThenExecutionTokenCreatedAutomatically()
    {
        // ARRANGE
        ExecutionsHelper executionsHelper = new ExecutionsHelper();
        executionsHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);

        // ACT
        boolean startedOk;
        ExecutionTokenStatus tokenStatusAfterStart;
        ExecutionTokenStatus tokenStatusAfterUsingBlock;

        try (TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10)))
        {
            startedOk = executionContext.tryStart();
            tokenStatusAfterStart = executionsHelper.getExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        waitFor(1000);
        tokenStatusAfterUsingBlock = executionsHelper.getExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

        // ASSERT
        assertTrue(startedOk);
        assertEquals(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
        assertEquals(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_InUsingBlock_ThenExecutionCompletedOnEndOfBlock()
    {
        // ARRANGE
        ExecutionsHelper executionsHelper = new ExecutionsHelper();
        int taskDefinitionId = executionsHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionsHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        // ACT
        boolean startedOk;
        ExecutionTokenStatus tokenStatusAfterStart;
        ExecutionTokenStatus tokenStatusAfterUsingBlock;

        try (TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10)))
        {
            startedOk = executionContext.tryStart();
            tokenStatusAfterStart = executionsHelper.getExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);
        }

        waitFor(1000);

        tokenStatusAfterUsingBlock = executionsHelper.getExecutionTokenStatus(TestConstants.ApplicationName, TestConstants.TaskName);

        // ASSERT
        assertTrue(startedOk);
        assertEquals(ExecutionTokenStatus.Unavailable, tokenStatusAfterStart);
        assertEquals(ExecutionTokenStatus.Available, tokenStatusAfterUsingBlock);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_KeepAlive_ThenKeepAliveContinuesUntilExecutionContextDies()
    {
        // ARRANGE
        ExecutionsHelper executionsHelper = new ExecutionsHelper();
        int taskDefinitionId = executionsHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionsHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        // ACT
        startContextWithoutUsingOrComplete();
        System.gc(); // referenceless context is collected
        waitFor(6000);

        // ASSERT
        Instant expectedLastKeepAliveMax = Instant.now().minusSeconds(5);
        Instant lastKeepAlive = executionsHelper.getLastKeepAlive(taskDefinitionId);
        assertTrue(lastKeepAlive.isBefore(expectedLastKeepAliveMax));
    }

    private void startContextWithoutUsingOrComplete()
    {
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        executionContext.tryStart();
    }

    private void waitFor(int milliseconds)
    {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {}
    }
}
