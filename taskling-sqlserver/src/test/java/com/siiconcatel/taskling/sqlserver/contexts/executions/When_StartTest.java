package com.siiconcatel.taskling.sqlserver.contexts.executions;

import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.core.tasks.TaskExecutionMeta;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.categories.TaskExecutionTests;
import com.siiconcatel.taskling.sqlserver.helpers.ClientHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class When_StartTest {
    private int taskDefinitionId;

    public When_StartTest()
    {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_TryStart_ThenLogCorrectTasklingVersion()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();

        // ACT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        boolean startedOk = executionContext.tryStart();
        String executionVersion = executionHelper.getLastExecutionVersion(taskDefinitionId);

        // ASSERT
        assertEquals("1.0", executionVersion);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_TryStartWithHeader_ThenGetHeaderReturnsTheHeader()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        MyHeader myHeader = new MyHeader("Jack",367);

        // ACT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        boolean startedOk = executionContext.tryStart(myHeader);
        assertTrue(startedOk);
        MyHeader myHeaderBack = executionContext.getHeader();

        // ASSERT
        assertEquals(myHeader.getName(), myHeaderBack.getName());
        assertEquals(myHeader.getId(), myHeaderBack.getId());
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_TryStartWithHeader_ThenHeaderWrittenToDatabase()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        MyHeader myHeader = new MyHeader("Jack",367);

        // ACT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        boolean startedOk = executionContext.tryStart(myHeader);
        assertTrue(startedOk);

        ExecutionsHelper dbHelper = new ExecutionsHelper();
        String executionHeader = dbHelper.getLastExecutionHeader(taskDefinitionId);
        String expectedHeader = "{\"name\":\"Jack\",\"id\":367}";

        // ASSERT
        assertEquals(expectedHeader, executionHeader);
    }
}
