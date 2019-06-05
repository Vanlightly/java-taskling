package com.siiconcatel.taskling.sqlserver.contexts.executions;

import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.categories.SlowTests;
import com.siiconcatel.taskling.sqlserver.categories.TaskExecutionTests;
import com.siiconcatel.taskling.sqlserver.helpers.ClientHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class When_BlockedTest {
    private int taskDefinitionId;

    public When_BlockedTest()
    {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_TryStartOverTheConcurrencyLimit_ThenMarkExecutionAsBlocked()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();

        // ACT
        boolean startedOk;
        boolean startedOkBlockedExec;
        boolean isBlocked;

        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        startedOk = executionContext.tryStart();
        TaskExecutionContext executionContextBlocked = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        startedOkBlockedExec = executionContextBlocked.tryStart();
        isBlocked = executionHelper.getBlockedStatusOfLastExecution(taskDefinitionId);
        executionContextBlocked.complete();
        executionContext.complete();

        // ASSERT
        assertTrue(isBlocked);
        assertTrue(startedOk);
        assertFalse(startedOkBlockedExec);

    }
}
