package com.siiconcatel.taskling.sqlserver.repositories.taskexecutionrepository;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.*;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.sqlserver.categories.ExecutionTokenTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.events.EventsRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.Pair;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskExecutionRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.CommonTokenRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenList;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenStatus;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.*;

public class When_TryStart_AsOverrideAfterElapsedTimeModeTest {
    public When_TryStart_AsOverrideAfterElapsedTimeModeTest()
    {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        TaskRepositoryMsSql.clearCache();
    }

    private TaskExecutionRepository createSut()
    {
        return new TaskExecutionRepositoryMsSql(new TaskRepositoryMsSql(), new ExecutionTokenRepositoryMsSql(new CommonTokenRepositoryMsSql()), new EventsRepositoryMsSql());
    }

    private TaskExecutionStartRequest createOverrideStartRequest(int concurrencyLimit)
    {
        TaskExecutionStartRequest request = new TaskExecutionStartRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), TaskDeathMode.Override, concurrencyLimit, 3, 3);
        request.setOverrideThreshold(Duration.ofMinutes(1));
        request.setTasklingVersion("N/A");
        return request;
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_ThenReturnsValidDataValues()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createOverrideStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse response = sut.start(startRequest);

        // ASSERT
        assertFalse(response.getTaskExecutionId().equals("0"));
        assertTrue(response.getStartedAt().isAfter(Instant.MIN));
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createOverrideStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse response = sut.start(startRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, response.getGrantStatus());
        assertNotEquals("0", response.getExecutionTokenId());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest firstStartRequest = createOverrideStartRequest(1);
        TaskExecutionStartRequest secondStartRequest = createOverrideStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstResponse = sut.start(firstStartRequest);
        TaskExecutionStartResponse secondResponse = sut.start(secondStartRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, firstResponse.getGrantStatus());
        assertEquals(GrantStatus.Denied, secondResponse.getGrantStatus());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest firstStartRequest = createOverrideStartRequest(1);
        TaskExecutionStartRequest secondStartRequest = createOverrideStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstStartResponse = sut.start(firstStartRequest);
        TaskExecutionCompleteRequest firstCompleteRequest = new TaskExecutionCompleteRequest(
                new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                firstStartResponse.getTaskExecutionId(),
                firstStartResponse.getExecutionTokenId());
        TaskExecutionCompleteResponse firstCompleteResponse = sut.complete(firstCompleteRequest);

        TaskExecutionStartResponse secondStartResponse = sut.start(secondStartRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, firstStartResponse.getGrantStatus());
        assertEquals(GrantStatus.Granted, secondStartResponse.getGrantStatus());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_FiveConcurrentTasksAndFourTokensAndAllAreAvailable_ThenIsGrantFirstFourTasksAndDenyTheOther()
    {
        // ARRANGE
        int concurrencyLimit = 4;
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, concurrencyLimit);

        TaskExecutionStartRequest firstStartRequest = createOverrideStartRequest(concurrencyLimit);
        TaskExecutionStartRequest secondStartRequest = createOverrideStartRequest(concurrencyLimit);
        TaskExecutionStartRequest thirdStartRequest = createOverrideStartRequest(concurrencyLimit);
        TaskExecutionStartRequest fourthStartRequest = createOverrideStartRequest(concurrencyLimit);
        TaskExecutionStartRequest fifthStartRequest = createOverrideStartRequest(concurrencyLimit);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstResponse = sut.start(firstStartRequest);
        executionHelper.setKeepAlive(firstResponse.getTaskExecutionId());
        TaskExecutionStartResponse secondResponse = sut.start(secondStartRequest);
        executionHelper.setKeepAlive(secondResponse.getTaskExecutionId());
        TaskExecutionStartResponse thirdResponse = sut.start(thirdStartRequest);
        executionHelper.setKeepAlive(thirdResponse.getTaskExecutionId());
        TaskExecutionStartResponse fourthResponse = sut.start(fourthStartRequest);
        executionHelper.setKeepAlive(fourthResponse.getTaskExecutionId());
        TaskExecutionStartResponse fifthResponse = sut.start(fifthStartRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, firstResponse.getGrantStatus());
        assertEquals(GrantStatus.Granted, secondResponse.getGrantStatus());
        assertEquals(GrantStatus.Granted, thirdResponse.getGrantStatus());
        assertEquals(GrantStatus.Granted, fourthResponse.getGrantStatus());
        assertEquals(GrantStatus.Denied, fifthResponse.getGrantStatus());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_OneToken_MultipleTaskThreads_ThenNoDeadLocksOccur()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        // ACT
        TaskExecutionRepository sut = createSut();
        List<TaskExecutionRepository> list = new ArrayList<>();
        for(int i=0; i<1000; i++)
            list.add(sut);

        list.parallelStream().forEach((repo) -> {
            requestAndReturnTokenWithTimeOverrideMode(repo);
        });
        
        // ASSERT

    }

    private void requestAndReturnTokenWithTimeOverrideMode(TaskExecutionRepository sut)
    {
        TaskExecutionStartRequest firstStartRequest = createOverrideStartRequest(1);

        TaskExecutionStartResponse firstStartResponse = sut.start(firstStartRequest);

        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.setKeepAlive(firstStartResponse.getTaskExecutionId());

        if (firstStartResponse.getGrantStatus() == GrantStatus.Granted)
        {
            TaskExecutionCompleteRequest firstCompleteRequest = new TaskExecutionCompleteRequest(
                    new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                    firstStartResponse.getTaskExecutionId(),
                    firstStartResponse.getExecutionTokenId());
            TaskExecutionCompleteResponse firstCompleteResponse = sut.complete(firstCompleteRequest);
        }
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_OneTaskAndOneTokenAndIsUnavailableAndGrantedDateHasPassedElapsedTime_ThenIsGranted()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createOverrideStartRequest(1);
        startRequest.setOverrideThreshold(Duration.ofSeconds(5));

        TaskExecutionStartRequest secondRequest = createOverrideStartRequest(1);
        secondRequest.setOverrideThreshold(Duration.ofSeconds(5));

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstResponse = sut.start(startRequest);
        executionHelper.setKeepAlive(firstResponse.getTaskExecutionId());

        waitFor(6000);

        TaskExecutionStartResponse secondResponse = sut.start(secondRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, firstResponse.getGrantStatus());
        assertEquals(GrantStatus.Granted, secondResponse.getGrantStatus());
        assertNotEquals("0", secondResponse.getExecutionTokenId());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_TimeOverrideMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createOverrideStartRequest(1);
        TaskExecutionStartRequest secondRequest = createOverrideStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstResponse = sut.start(startRequest);

        waitFor(5000);

        TaskExecutionStartResponse secondResponse = sut.start(secondRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, firstResponse.getGrantStatus());
        assertNotEquals("0", firstResponse.getExecutionTokenId());
        assertEquals(GrantStatus.Denied, secondResponse.getGrantStatus());
        assertEquals("0", secondResponse.getExecutionTokenId());
    }

    private void waitFor(int milliseconds)
    {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {}
    }
}
