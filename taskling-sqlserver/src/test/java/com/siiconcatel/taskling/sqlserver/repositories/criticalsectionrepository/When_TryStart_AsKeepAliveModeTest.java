package com.siiconcatel.taskling.sqlserver.repositories.criticalsectionrepository;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.CriticalSectionRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.CriticalSectionType;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.StartCriticalSectionRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.StartCriticalSectionResponse;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.GrantStatus;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.sqlserver.categories.CriticalSectionTokens;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.CommonTokenRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.criticalsections.CriticalSectionRepositoryMsSql;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class When_TryStart_AsKeepAliveModeTest {
    public When_TryStart_AsKeepAliveModeTest()
    {
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);
    }

    private CriticalSectionRepository createSut()
    {
        return new CriticalSectionRepositoryMsSql(new TaskRepositoryMsSql(), new CommonTokenRepositoryMsSql());
    }

    @Category({FastTests.class, CriticalSectionTokens.class})
    @Test
    public void If_KeepAliveMode_TokenAvailableAndNothingInQueue_ThenGrant()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        String taskExecutionId = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

         StartCriticalSectionRequest request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecutionId,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
        request. setKeepAliveDeathThreshold(Duration.ofMinutes(1));

        // ACT
        CriticalSectionRepository sut = createSut();
        StartCriticalSectionResponse response = sut.start(request);

        // ASSERT
        assertEquals(GrantStatus.Granted, response.getGrantStatus());
    }

    @Category({FastTests.class, CriticalSectionTokens.class})
    @Test
    public void If_KeepAliveMode_TokenNotAvailableAndNothingInQueue_ThenAddToQueueAndDeny()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and assign critical section to it
        String taskExecutionId1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.insertUnavailableCriticalSectionToken(taskDefinitionId, taskExecutionId1);

        // Create second execution
        String taskExecutionId2 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);

        StartCriticalSectionRequest request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
        request.setKeepAliveDeathThreshold(Duration.ofMinutes(1));

        // ACT
        CriticalSectionRepository sut = createSut();
        StartCriticalSectionResponse response = sut.start(request);

        // ASSERT
        boolean isInQueue = executionHelper.getQueueCount(taskExecutionId2) == 1;
        assertTrue(isInQueue);
        assertEquals(GrantStatus.Denied, response.getGrantStatus());
    }

    @Category({FastTests.class, CriticalSectionTokens.class})
    @Test
    public void If_KeepAliveMode_TokenNotAvailableAndAlreadyInQueue_ThenDoNotAddToQueueAndDeny()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and assign critical section to it
        String taskExecutionId1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.insertUnavailableCriticalSectionToken(taskDefinitionId, taskExecutionId1);

        // Create second execution and insert into queue
        String taskExecutionId2 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId2);

         StartCriticalSectionRequest request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
        request.setKeepAliveDeathThreshold(Duration.ofMinutes(10));

        // ACT
         CriticalSectionRepository sut = createSut();
         StartCriticalSectionResponse response = sut.start(request);

        // ASSERT
        int numberOfQueueRecords = executionHelper.getQueueCount(taskExecutionId2);
        assertEquals(1, numberOfQueueRecords);
        assertEquals(GrantStatus.Denied, response.getGrantStatus());
    }

    @Category({FastTests.class, CriticalSectionTokens.class})
    @Test
    public void If_KeepAliveMode_TokenAvailableAndIsFirstInQueue_ThenRemoveFromQueueAndGrant()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and create available critical section token
        String taskExecutionId1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);
        executionHelper.insertAvailableCriticalSectionToken(taskDefinitionId, "0");

         StartCriticalSectionRequest request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecutionId1,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
        request. setKeepAliveDeathThreshold(Duration.ofMinutes(1));

        // ACT
         CriticalSectionRepository sut = createSut();
         StartCriticalSectionResponse response = sut.start(request);

        // ASSERT
        int numberOfQueueRecords = executionHelper.getQueueCount(taskExecutionId1);
        assertEquals(0, numberOfQueueRecords);
        assertEquals(GrantStatus.Granted, response.getGrantStatus());
    }

    @Category({FastTests.class, CriticalSectionTokens.class})
    @Test
    public void If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueue_ThenDoNotChangeQueueAndDeny()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and add it to the queue
        String taskExecutionId1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);

        // Create execution 2 and add it to the queue
        String taskExecutionId2 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

        // Create an available critical section token
        executionHelper.insertAvailableCriticalSectionToken(taskDefinitionId, "0");

        StartCriticalSectionRequest request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
        request.setKeepAliveDeathThreshold(Duration.ofMinutes(1));

        // ACT
         CriticalSectionRepository sut = createSut();
         StartCriticalSectionResponse response = sut.start(request);

        // ASSERT
        int numberOfQueueRecords = executionHelper.getQueueCount(taskExecutionId2);
        assertEquals(1, numberOfQueueRecords);
        assertEquals(GrantStatus.Denied, response.getGrantStatus());
    }

    @Category({FastTests.class, CriticalSectionTokens.class})
    @Test
    public void If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueueButFirstHasExpiredTimeout_ThenRemoveBothFromQueueAndGrant()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);

        Duration keepAliveThreshold = Duration.ofSeconds(5);

        // Create execution 1 and add it to the queue
        String taskExecutionId1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId, Duration.ofSeconds(1), keepAliveThreshold);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);
        executionHelper.setKeepAlive(taskExecutionId1);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);

        waitFor(6000);

        // Create execution 2 and add it to the queue
        String taskExecutionId2 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.setKeepAlive(taskExecutionId2);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

        // Create an available critical section token
        executionHelper.insertAvailableCriticalSectionToken(taskDefinitionId, "0");

         StartCriticalSectionRequest request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
        request.setKeepAliveDeathThreshold(keepAliveThreshold);

        // ACT
         CriticalSectionRepository sut = createSut();
         StartCriticalSectionResponse response = sut.start(request);

        // ASSERT
        int numberOfQueueRecordsForExecution1 = executionHelper.getQueueCount(taskExecutionId1);
        int numberOfQueueRecordsForExecution2 = executionHelper.getQueueCount(taskExecutionId2);
        assertEquals(0, numberOfQueueRecordsForExecution1);
        assertEquals(0, numberOfQueueRecordsForExecution2);
        assertEquals(GrantStatus.Granted, response.getGrantStatus());
    }

    @Category({FastTests.class, CriticalSectionTokens.class})
    @Test
    public void If_KeepAliveMode_TokenAvailableAndIsNotFirstInQueueButFirstHasCompleted_ThenRemoveBothFromQueueAndGrant()
    {
        // ARRANGE
        ExecutionsHelper executionHelper = new ExecutionsHelper();
        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        // Create execution 1 and add it to the queue
        String taskExecutionId1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.setKeepAlive(taskExecutionId1);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 1, taskExecutionId1);
        executionHelper.setTaskExecutionAsCompleted(taskExecutionId1);

        // Create execution 2 and add it to the queue
        String taskExecutionId2 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId);
        executionHelper.setKeepAlive(taskExecutionId2);
        executionHelper.insertIntoCriticalSectionQueue(taskDefinitionId, 2, taskExecutionId2);

        // Create an available critical section token
        executionHelper.insertAvailableCriticalSectionToken(taskDefinitionId, "0");

         StartCriticalSectionRequest request = new StartCriticalSectionRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecutionId2,
                TaskDeathMode.KeepAlive,
                CriticalSectionType.User);
        request.setKeepAliveDeathThreshold(Duration.ofMinutes(30));

        // ACT
        CriticalSectionRepository sut = createSut();
        StartCriticalSectionResponse response = sut.start(request);

        // ASSERT
        int numberOfQueueRecordsForExecution1 = executionHelper.getQueueCount(taskExecutionId1);
        int numberOfQueueRecordsForExecution2 = executionHelper.getQueueCount(taskExecutionId2);
        assertEquals(0, numberOfQueueRecordsForExecution1);
        assertEquals(0, numberOfQueueRecordsForExecution2);
        assertEquals(GrantStatus.Granted, response.getGrantStatus());
    }

    private void waitFor(int milliseconds)
    {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {}
    }
}
