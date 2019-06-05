package com.siiconcatel.taskling.sqlserver.contexts.executions;

import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.core.tasks.TaskExecutionMeta;
import com.siiconcatel.taskling.core.tasks.TaskExecutionMetaWithHeader;
import com.siiconcatel.taskling.core.tasks.TaskExecutionStatus;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.categories.TaskExecutionTests;
import com.siiconcatel.taskling.sqlserver.helpers.ClientHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.LastEvent;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.util.List;

import static junit.framework.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class When_GetLastExecutionMetaTest {
    private int taskDefinitionId;

    public When_GetLastExecutionMetaTest()
    {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_MultipleExecutionsAndGetLastExecutionMeta_ThenReturnLastOne()
    {
        // ARRANGE

        for (int i = 0; i < 5; i++)
        {
            TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
            executionContext.tryStart("My reference value" + i);
            executionContext.complete();
            waitFor(200);
        }

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMeta executionMeta = executionContext.getLastExecutionMeta();
        assertEquals("My reference value4", executionMeta.getReferenceValue());
        executionContext.complete();
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_MultipleExecutionsAndGetLastExecutionMetas_ThenReturnLastXItems()
    {
        // ARRANGE

        for (int i = 0; i < 5; i++)
        {
            TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
            executionContext.tryStart("My reference value" + i);
            executionContext.complete();
            waitFor(200);
        }

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        List<TaskExecutionMeta> executionMetas = executionContext.getLastExecutionMetas(3);

        assertEquals(3, executionMetas.size());
        assertEquals("My reference value4", executionMetas.get(0).getReferenceValue());
        assertEquals("My reference value3", executionMetas.get(1).getReferenceValue());
        assertEquals("My reference value2", executionMetas.get(2).getReferenceValue());
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_NoPreviousExecutionsAndGetLastExecutionMeta_ThenReturnNull()
    {
        // ARRANGE

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMeta executionMeta = executionContext.getLastExecutionMeta();
        assertNull(executionMeta);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_MultipleExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnLastOne()
    {
        // ARRANGE

        for (int i = 0; i < 5; i++)
        {
            MyHeader myHeader = new MyHeader("Test", i);

            TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
            executionContext.tryStart(myHeader);
            executionContext.complete();
            waitFor(200);
        }

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMetaWithHeader<MyHeader> executionMeta = executionContext.getLastExecutionMetaWithHeader(MyHeader.class);
        assertEquals(4, executionMeta.getHeader().getId());
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_MultipleExecutionsAndGetLastExecutionMetasWithHeader_ThenReturnLastXItems()
    {
        // ARRANGE

        for (int i = 0; i < 5; i++)
        {
            MyHeader myHeader = new MyHeader("Test", i);

            TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
            executionContext.tryStart(myHeader);
            executionContext.complete();
            waitFor(200);
        }

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        List<TaskExecutionMetaWithHeader<MyHeader>> executionMetas = executionContext.getLastExecutionMetasWithHeader(MyHeader.class, 3);
        assertEquals(3, executionMetas.size());
        assertEquals(4, executionMetas.get(0).getHeader().getId());
        assertEquals(3, executionMetas.get(1).getHeader().getId());
        assertEquals(2, executionMetas.get(2).getHeader().getId());
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_NoPreviousExecutionsAndGetLastExecutionMetaWithHeader_ThenReturnNull()
    {
        // ARRANGE
        // ACT

        // ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMetaWithHeader<MyHeader> executionMeta = executionContext.getLastExecutionMetaWithHeader(MyHeader.class);
        assertNull(executionMeta);
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_LastExecutionCompleted_ThenReturnStatusIsCompleted()
    {
        // ARRANGE
        TaskExecutionContext prev = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        prev.tryStart();
        prev.complete();
        waitFor(200);

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMeta executionMeta = executionContext.getLastExecutionMeta();
        assertEquals(TaskExecutionStatus.Completed, executionMeta.getStatus());
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_LastExecutionFailed_ThenReturnStatusIsFailed()
    {
        // ARRANGE
        TaskExecutionContext prev = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        prev.tryStart();
        prev.error("", true);
        prev.complete();
        waitFor(200);

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMeta executionMeta = executionContext.getLastExecutionMeta();
        assertEquals(TaskExecutionStatus.Failed, executionMeta.getStatus());
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_LastExecutionBlocked_ThenReturnStatusIsBlockedAsync()
    {
        // ARRANGE
        TaskExecutionContext first = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        first.tryStart();
        waitFor(200);

        TaskExecutionContext second = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        second.tryStart();
        second.complete();

        first.complete();

        // ACT and ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMeta executionMeta = executionContext.getLastExecutionMeta();
        assertEquals(TaskExecutionStatus.Blocked, executionMeta.getStatus());
        executionContext.complete();
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_LastExecutionInProgress_ThenReturnStatusIsInProgress()
    {
        // ARRANGE, ACT, ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        executionContext.tryStart();
        waitFor(200);

        TaskExecutionContext executionContext2 = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMeta executionMeta = executionContext2.getLastExecutionMeta();
        assertEquals(TaskExecutionStatus.InProgress, executionMeta.getStatus());
    }

    @Category({FastTests.class, TaskExecutionTests.class})
    @Test
    public void If_LastExecutionDead_ThenReturnStatusIsDead()
    {
        // ARRANGE, ACT, ASSERT
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        executionContext.tryStart();
        executionContext.complete();
        waitFor(200);
        ExecutionsHelper helper = new ExecutionsHelper();
        helper.setLastExecutionAsDead(taskDefinitionId);

        TaskExecutionContext executionContext2 = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10));
        TaskExecutionMeta executionMeta = executionContext2.getLastExecutionMeta();
        assertEquals(TaskExecutionStatus.Dead, executionMeta.getStatus());
    }

    private void waitFor(int milliseconds)
    {
        try {
            Thread.sleep(200);
        }
        catch (InterruptedException e) {}
    }
}
