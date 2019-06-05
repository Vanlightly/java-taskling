package com.siiconcatel.taskling.sqlserver.contexts.executions;

import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.categories.TaskExecutionTests;
import com.siiconcatel.taskling.sqlserver.helpers.ClientHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.LastEvent;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class When_CheckpointTest {
    private int taskDefinitionId;

    public When_CheckpointTest()
    {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_Checkpoint_ThenCheckpointEventCreated()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();

        // ACT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        boolean startedOk = executionContext.tryStart();
        executionContext.checkpoint("Test checkpoint");
        LastEvent lastEvent = executionHelper.getLastEvent(taskDefinitionId);

        // ASSERT
        assertEquals(EventType.CheckPoint, lastEvent.Type);
        assertEquals("Test checkpoint", lastEvent.Description);
    }
}
