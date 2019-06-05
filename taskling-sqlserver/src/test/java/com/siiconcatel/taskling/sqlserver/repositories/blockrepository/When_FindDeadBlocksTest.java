package com.siiconcatel.taskling.sqlserver.repositories.blockrepository;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindDeadBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.sqlserver.blocks.BlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.helpers.BlocksHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class When_FindDeadBlocksTest {
    private int taskDefinitionId;
    private String taskExecution1;
    private String block1;
    private String block2;
    private String block3;
    private String block4;
    private String block5;

    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;

    private Duration FiveMinuteSpan = Duration.ofMinutes(5);
    private Duration OneMinuteSpan = Duration.ofMinutes(1);
    private Duration TwentySecondSpan = Duration.ofSeconds(20);

    public When_FindDeadBlocksTest()
    {
        blocksHelper = new BlocksHelper();
        blocksHelper.deleteBlocks(TestConstants.ApplicationName);
        executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);

        TaskRepositoryMsSql.clearCache();
    }

    private void insertDateRangeTestData(TaskDeathMode taskDeathMode)
    {
        Instant now = Instant.now();
        if (taskDeathMode == TaskDeathMode.Override)
            taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, OneMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));
        else
            taskExecution1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));

        insertDateRangeBlocksTestData();
    }

    private void insertDateRangeBlocksTestData()
    {
        Instant now = Instant.now();
        block1 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(180*60), now.minusSeconds(179*60)));
        block2 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(200*60), now.minusSeconds(199*60)));
        block3 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(220*60), now.minusSeconds(219*60)));
        block4 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(240*60), now.minusSeconds(239*60)));
        block5 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(250*60), now.minusSeconds(249*60)));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block1), now.minusSeconds(180*60), now.minusSeconds(180*60), now.minusSeconds(175*60), BlockExecutionStatus.Failed, 2);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block2), now.minusSeconds(200*60), now.minusSeconds(200*60), null, BlockExecutionStatus.Started, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block3), now.minusSeconds(220*60), null, null, BlockExecutionStatus.NotStarted, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block4), now.minusSeconds(240*60), now.minusSeconds(240*60), now.minusSeconds(235*60), BlockExecutionStatus.Completed, 2);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block5), now.minusSeconds(250*60), now.minusSeconds(250*60), null, BlockExecutionStatus.Started, 3);
    }

    private void insertNumericRangeTestData(TaskDeathMode taskDeathMode)
    {
        Instant now = Instant.now();
        if (taskDeathMode == TaskDeathMode.Override)
            taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, OneMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));
        else
            taskExecution1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));

        insertNumericRangeBlocksTestData();
    }

    private void insertNumericRangeBlocksTestData()
    {
        Instant now = Instant.now();
        block1 = String.valueOf(blocksHelper.insertNumericRangeBlock(taskDefinitionId, 1, 100, now.minusSeconds(100*60)));
        block2 = String.valueOf(blocksHelper.insertNumericRangeBlock(taskDefinitionId, 101, 200, now.minusSeconds(90*60)));
        block3 = String.valueOf(blocksHelper.insertNumericRangeBlock(taskDefinitionId, 201, 300, now.minusSeconds(80*60)));
        block4 = String.valueOf(blocksHelper.insertNumericRangeBlock(taskDefinitionId, 301, 400, now.minusSeconds(70*60)));
        block5 = String.valueOf(blocksHelper.insertNumericRangeBlock(taskDefinitionId, 401, 500, now.minusSeconds(60*60)));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block1), now.minusSeconds(180*60), now.minusSeconds(180*60), now.minusSeconds(175*60), BlockExecutionStatus.Failed, 2);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block2), now.minusSeconds(200*60), now.minusSeconds(200*60), null, BlockExecutionStatus.Started, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block3), now.minusSeconds(220*60), null, null, BlockExecutionStatus.NotStarted, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block4), now.minusSeconds(240*60), now.minusSeconds(240*60), now.minusSeconds(235*60), BlockExecutionStatus.Completed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block5), now.minusSeconds(250*60), now.minusSeconds(250*60), null, BlockExecutionStatus.Started, 3);
    }

    private void insertListTestData(TaskDeathMode taskDeathMode)
    {
        Instant now = Instant.now();
        if (taskDeathMode == TaskDeathMode.Override)
            taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, OneMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));
        else
            taskExecution1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));

        insertListBlocksTestData();
    }

    private void insertListBlocksTestData()
    {
        Instant now = Instant.now();
        block1 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(246*60), null));
        block2 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(247*60), null));
        block3 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(248*60), null));
        block4 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(249*60), null));
        block5 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(250*60), null));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block1), now.minusSeconds(180*60), now.minusSeconds(180*60), now.minusSeconds(175*60), BlockExecutionStatus.Failed, 2);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block2), now.minusSeconds(200*60), now.minusSeconds(200*60), null, BlockExecutionStatus.Started, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block3), now.minusSeconds(220*60), null, null, BlockExecutionStatus.NotStarted, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block4), now.minusSeconds(240*60), now.minusSeconds(240*60), now.minusSeconds(235*60), BlockExecutionStatus.Completed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block5), now.minusSeconds(250*60), now.minusSeconds(250*60), null, BlockExecutionStatus.Started, 3);
    }

    private void insertObjectTestData(TaskDeathMode taskDeathMode)
    {
        Instant now = Instant.now();
        if (taskDeathMode == TaskDeathMode.Override)
            taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, OneMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));
        else
            taskExecution1 = executionHelper.insertKeepAliveTaskExecution(taskDefinitionId, TwentySecondSpan, FiveMinuteSpan, now.minusSeconds(250*60), now.minusSeconds(179*60));

        insertObjectBlocksTestData();
    }

    private void insertObjectBlocksTestData()
    {
        Instant now = Instant.now();
        block1 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(246*60), UUID.randomUUID().toString()));
        block2 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(247*60), UUID.randomUUID().toString()));
        block3 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(248*60), UUID.randomUUID().toString()));
        block4 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(249*60), UUID.randomUUID().toString()));
        block5 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(250*60), UUID.randomUUID().toString()));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block1), now.minusSeconds(180*60), now.minusSeconds(180*60), now.minusSeconds(175*60), BlockExecutionStatus.Failed, 2);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block2), now.minusSeconds(200*60), now.minusSeconds(200*60), null, BlockExecutionStatus.Started, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block3), now.minusSeconds(220*60), null, null, BlockExecutionStatus.NotStarted, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block4), now.minusSeconds(240*60), now.minusSeconds(240*60), now.minusSeconds(235*60), BlockExecutionStatus.Completed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block5), now.minusSeconds(250*60), now.minusSeconds(250*60), null, BlockExecutionStatus.Started, 3);
    }

    private BlockRepository createSut()
    {
        return new BlockRepositoryMsSql(new TaskRepositoryMsSql());
    }


    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndDateRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndDateRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 1;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(1, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndDateRange_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        int retryLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.Override, blockCountLimit, retryLimit, 300);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndDateRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(2*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndDateRange_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertDateRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(50*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.DateRange, TaskDeathMode.KeepAlive, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndNumericRange_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndNumericRange_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 1;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(1, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndNumericRange_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        int retryLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.Override, blockCountLimit, retryLimit, 300);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(block3)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndNumericRange_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(2*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndNumericRange_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertNumericRangeTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(50*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.NumericRange, TaskDeathMode.KeepAlive, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> deadBlocks = sut.findDeadRangeBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndList_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndList_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndList_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.Override);
        int blockCountLimit = 1;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(1, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndList_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        int attemptLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.Override, blockCountLimit, attemptLimit, 300);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block3)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getListBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndList_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(2*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndList_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertListTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(50*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.List, TaskDeathMode.KeepAlive, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> deadBlocks = sut.findDeadListBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndObject_DeadTasksInTargetPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndObject_DeadTasksOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndObject_DeadTasksInTargetPeriodAndMoreThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.Override);
        int blockCountLimit = 1;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(1, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_OverrideModeAndObject_SomeDeadTasksHaveReachedRetryLimit_ThenReturnOnlyDeadBlocksNotAtLimit()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.Override);
        int blockCountLimit = 5;
        int attemptLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.Override, blockCountLimit, attemptLimit, 300);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block3)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitPeriodAndLessThanBlockCountLimit_ThenReturnAllDeadBlocks()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(3, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block2)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitAndGreaterThanBlockCountLimit_ThenReturnOldestDeadBlocksUpToLimit()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(250*60));

        int blockCountLimit = 2;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(2, deadBlocks.size());
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block3)));
        assertTrue(deadBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(block5)));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndObject_DeadTasksNotPassedKeepAliveLimitInTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(2*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_KeepAliveModeAndObject_DeadTasksPassedKeepAliveLimitButAreOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        insertObjectTestData(TaskDeathMode.KeepAlive);
        executionHelper.setKeepAlive(taskExecution1, Instant.now().minusSeconds(50*60));

        int blockCountLimit = 5;
        FindDeadBlocksRequest request = createDeadBlockRequest(BlockType.Object, TaskDeathMode.KeepAlive, blockCountLimit, 3, 100);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> deadBlocks = sut.findDeadObjectBlocks(request);

        // ASSERT
        assertEquals(0, deadBlocks.size());
    }


    private FindDeadBlocksRequest createDeadBlockRequest(BlockType blockType, TaskDeathMode taskDeathMode, int blockCountLimit)
    {
        return createDeadBlockRequest(blockType, taskDeathMode, blockCountLimit, 3, 300);
    }

    private FindDeadBlocksRequest createDeadBlockRequest(BlockType blockType, TaskDeathMode taskDeathMode, 
                                                         int blockCountLimit, int attemptLimit, int fromMinutesBack)
    {
        return new FindDeadBlocksRequest(
                new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                blockType,
                Instant.now().minusSeconds(fromMinutesBack*60),
                Instant.now(),
                blockCountLimit,
                taskDeathMode,
                attemptLimit);
    }
}
