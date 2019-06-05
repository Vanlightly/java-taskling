package com.siiconcatel.taskling.sqlserver.repositories.taskexecutionrepository;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.*;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
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

public class When_TryStart_AsKeepAliveModeTest {
    public When_TryStart_AsKeepAliveModeTest()
    {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        TaskRepositoryMsSql.clearCache();
    }

    private TaskExecutionRepository createSut()
    {
        return new TaskExecutionRepositoryMsSql(new TaskRepositoryMsSql(), new ExecutionTokenRepositoryMsSql(new CommonTokenRepositoryMsSql()), new EventsRepositoryMsSql());
    }

    private TaskExecutionStartRequest createKeepAliveStartRequest(int concurrencyLimit)
    {
        TaskExecutionStartRequest request = new TaskExecutionStartRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), TaskDeathMode.KeepAlive, concurrencyLimit, 3, 3);
        request.setKeepAliveDeathThreshold(Duration.ofMinutes(1));
        request.setKeepAliveInterval(Duration.ofSeconds(20));
        request.setTasklingVersion("N/A");

        return request;
    }

    private SendKeepAliveRequest createKeepAliveRequest(String applicationName, String taskName, String taskExecutionId, String executionTokenId)
    {
        SendKeepAliveRequest request = new SendKeepAliveRequest();
        request.setTaskId(new TaskId(applicationName, taskName));
        request.setTaskExecutionId(taskExecutionId);
        request.setExecutionTokenId(executionTokenId);

        return request;
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_ThenReturnsValidDataValues()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse response = sut.start(startRequest);

        // ASSERT
        assertFalse(response.getTaskExecutionId().equals("0"));
        assertTrue(response.getStartedAt().isAfter(Instant.MIN));
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_OneTaskAndOneTokenAndIsAvailable_ThenIsGranted()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse response = sut.start(startRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, response.getGrantStatus());
        assertNotEquals("0", response.getExecutionTokenId());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_TwoConcurrentTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndDenyTheOther()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest firstStartRequest = createKeepAliveStartRequest(1);
        TaskExecutionStartRequest secondStartRequest = createKeepAliveStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstResponse = sut.start(firstStartRequest);
        sut.sendKeepAlive(createKeepAliveRequest(TestConstants.ApplicationName, TestConstants.TaskName, firstResponse.getTaskExecutionId(), firstResponse.getExecutionTokenId()));
        TaskExecutionStartResponse secondResponse = sut.start(secondStartRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, firstResponse.getGrantStatus());
        assertEquals(GrantStatus.Denied, secondResponse.getGrantStatus());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_TwoSequentialTasksAndOneTokenAndIsAvailable_ThenIsGrantFirstTaskAndThenGrantTheOther()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest firstStartRequest = createKeepAliveStartRequest(1);
        TaskExecutionStartRequest secondStartRequest = createKeepAliveStartRequest(1);

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
    public void If_KeepAliveMode_FiveConcurrentTasksAndFourTokensAndAllAreAvailable_ThenIsGrantFirstFourTasksAndDenyTheOther()
    {
        // ARRANGE
        int concurrencyLimit = 4;
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, concurrencyLimit);

        TaskExecutionStartRequest firstStartRequest = createKeepAliveStartRequest(concurrencyLimit);
        TaskExecutionStartRequest secondStartRequest = createKeepAliveStartRequest(concurrencyLimit);
        TaskExecutionStartRequest thirdStartRequest = createKeepAliveStartRequest(concurrencyLimit);
        TaskExecutionStartRequest fourthStartRequest = createKeepAliveStartRequest(concurrencyLimit);
        TaskExecutionStartRequest fifthStartRequest = createKeepAliveStartRequest(concurrencyLimit);

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
    public void If_KeepAliveMode_OneToken_MultipleTaskThreads_ThenNoDeadLocksOccur()
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
            requestAndReturnTokenWithKeepAliveMode(repo);
        });
        
        // ASSERT

    }

    private void requestAndReturnTokenWithKeepAliveMode(TaskExecutionRepository sut)
    {
        TaskExecutionStartRequest firstStartRequest = createKeepAliveStartRequest(1);

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
    public void If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasPassedElapsedTime_ThenIsGranted()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(1);
        startRequest.setKeepAliveDeathThreshold(Duration.ofSeconds(4));

        TaskExecutionStartRequest secondRequest = createKeepAliveStartRequest(1);
        secondRequest.setKeepAliveDeathThreshold(Duration.ofSeconds(4));

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstResponse = sut.start(startRequest);
        executionHelper.setKeepAlive(firstResponse.getTaskExecutionId());

        waitFor(6000);

        TaskExecutionStartResponse secondResponse = sut.start(secondRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, secondResponse.getGrantStatus());
        assertNotEquals("0", secondResponse.getExecutionTokenId());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_OneTaskAndOneTokenAndIsUnavailableAndKeepAliveHasNotPassedElapsedTime_ThenIsDenied()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(1);
        startRequest.setKeepAliveDeathThreshold(Duration.ofHours(1));

        TaskExecutionStartRequest secondRequest = createKeepAliveStartRequest(1);
        secondRequest.setKeepAliveDeathThreshold(Duration.ofHours(1));

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse firstResponse = sut.start(startRequest);
        executionHelper.setKeepAlive(firstResponse.getTaskExecutionId());

        waitFor(5000);

        TaskExecutionStartResponse secondResponse = sut.start(secondRequest);

        // ASSERT
        assertEquals(GrantStatus.Granted, firstResponse.getGrantStatus());
        assertNotEquals("0", firstResponse.getExecutionTokenId());
        assertEquals(GrantStatus.Denied, secondResponse.getGrantStatus());
        assertEquals("0", secondResponse.getExecutionTokenId());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsFour_ThenCreateThreeNewTokens()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(4);

        // ACT
        TaskExecutionRepository sut = createSut();
        sut.start(startRequest);

        // ASSERT
        ExecutionTokenList tokensList = executionHelper.getExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
        assertEquals(1, tokensList.getTokens().stream().filter(x -> x.getStatus() == ExecutionTokenStatus.Unavailable).count());
        assertEquals(3, tokensList.getTokens().stream().filter(x -> x.getStatus() == ExecutionTokenStatus.Available).count());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_OneTokenExistsAndConcurrencyLimitIsUnlimited_ThenRemoveAvailableTokenAndCreateOneNewUnlimitedToken()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(-1);

        // ACT
        TaskExecutionRepository sut = createSut();
        sut.start(startRequest);

        // ASSERT
        ExecutionTokenList tokensList = executionHelper.getExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
        assertEquals(1, tokensList.getTokens().stream().filter(x -> x.getStatus() == ExecutionTokenStatus.Unlimited).count());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_OneAvailableTokenAndOneUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveAvailableToken_AndSoDenyStart()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        List<Pair<ExecutionTokenStatus, String>> tokenList =  new ArrayList<>();
        tokenList.add(new Pair<>(ExecutionTokenStatus.Unavailable, "0"));
        tokenList.add(new Pair<>(ExecutionTokenStatus.Available, "1"));
        executionHelper.insertExecutionToken(taskDefinitionId, tokenList);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        TaskExecutionStartResponse result = sut.start(startRequest);

        // ASSERT
        ExecutionTokenList tokensList = executionHelper.getExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
        assertEquals(GrantStatus.Denied, result.getGrantStatus());
        assertEquals(1, tokensList.getTokens().stream().filter(x -> x.getStatus() == ExecutionTokenStatus.Unavailable).count());
    }

    @Category({FastTests.class, ExecutionTokenTests.class})
    @Test
    public void If_KeepAliveMode_TwoUnavailableTokensExistsAndConcurrencyLimitIsOne_ThenRemoveOneUnavailableToken()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        List<Pair<ExecutionTokenStatus, String>> tokenList =  new ArrayList<>();
        tokenList.add(new Pair<>(ExecutionTokenStatus.Unavailable, "0"));
        tokenList.add(new Pair<>(ExecutionTokenStatus.Unavailable, "1"));
        executionHelper.insertExecutionToken(taskDefinitionId, tokenList);

        TaskExecutionStartRequest startRequest = createKeepAliveStartRequest(1);

        // ACT
        TaskExecutionRepository sut = createSut();
        sut.start(startRequest);

        // ASSERT
        ExecutionTokenList tokensList = executionHelper.getExecutionTokens(TestConstants.ApplicationName, TestConstants.TaskName);
        assertEquals(1, tokensList.getTokens().stream().filter(x -> x.getStatus() == ExecutionTokenStatus.Unavailable).count());
    }

    private void waitFor(int milliseconds)
    {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {}
    }
}
