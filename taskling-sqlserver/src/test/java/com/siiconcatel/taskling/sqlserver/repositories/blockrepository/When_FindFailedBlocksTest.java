package com.siiconcatel.taskling.sqlserver.repositories.blockrepository;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindFailedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;
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

public class When_FindFailedBlocksTest {
    private int taskDefinitionId;
    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;

    public When_FindFailedBlocksTest()
    {
        executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);
        blocksHelper = new BlocksHelper();
        blocksHelper.deleteBlocks(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);
    }

    private BlockRepository createSut()
    {
        return new BlockRepositoryMsSql(new TaskRepositoryMsSql());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedDateRangeBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*12), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*2), now.minusSeconds(60*1));
        long block2 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*12), now.minusSeconds(60*11));
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.DateRange,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> failedBlocks = sut.findFailedRangeBlocks(request);

        // ASSERT
        assertEquals(1, failedBlocks.size());
        assertEquals(String.valueOf(block1), failedBlocks.get(0).getRangeBlockId());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void  When_FailedDateRangeBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*32), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*2), now.minusSeconds(60*1));
        long block2 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*12), now.minusSeconds(60*11));
        long block3 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*22), now.minusSeconds(60*21));
        long block4 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*32), now.minusSeconds(60*31));
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block3, now.minusSeconds(60*22), now.minusSeconds(60*22), now.minusSeconds(60*21), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block4, now.minusSeconds(60*32), now.minusSeconds(60*32), now.minusSeconds(60*31), BlockExecutionStatus.Completed, 1);

        int blockCountLimit = 2;

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.DateRange,
                Instant.now().minusSeconds(60*200),
                Instant.now(),
                blockCountLimit,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> failedBlocks = sut.findFailedRangeBlocks(request);

        // ASSERT
        assertEquals(blockCountLimit, failedBlocks.size());
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(String.valueOf(block2))));
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(String.valueOf(block3))));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedDateRangeBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*212), now.minusSeconds(60*200));
        long block1 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*200), now.minusSeconds(60*201));
        long block2 = blocksHelper.insertDateRangeBlock(taskDefinitionId, now.minusSeconds(60*212), now.minusSeconds(60*211));
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*200), now.minusSeconds(60*200), now.minusSeconds(60*201), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*212), now.minusSeconds(60*212), now.minusSeconds(60*211), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.DateRange,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> failedBlocks = sut.findFailedRangeBlocks(request);

        // ASSERT
        assertEquals(0, failedBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedNumericRangeBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*12), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 1, 2, now.minusSeconds(60*2));
        long block2 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 3, 4, now.minusSeconds(60*12));
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.NumericRange,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> failedBlocks = sut.findFailedRangeBlocks(request);

        // ASSERT
        assertEquals(1, failedBlocks.size());
        assertEquals(String.valueOf(block1), failedBlocks.get(0).getRangeBlockId());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedNumericRangeBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*32), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 1, 2, now.minusSeconds(60*2));
        long block2 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 3, 4, now.minusSeconds(60*12));
        long block3 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 5, 6, now.minusSeconds(60*22));
        long block4 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 7, 8, now.minusSeconds(60*32));
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block3, now.minusSeconds(60*22), now.minusSeconds(60*22), now.minusSeconds(60*21), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block4, now.minusSeconds(60*32), now.minusSeconds(60*32), now.minusSeconds(60*31), BlockExecutionStatus.Completed, 1);

        int blockCountLimit = 2;

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.NumericRange,
                Instant.now().minusSeconds(60*200),
                Instant.now(),
                blockCountLimit,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> failedBlocks = sut.findFailedRangeBlocks(request);

        // ASSERT
        assertEquals(blockCountLimit, failedBlocks.size());
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(String.valueOf(block2))));
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getRangeBlockId().equals(String.valueOf(block3))));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedNumericRangeBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*212), now.minusSeconds(60*200));
        long block1 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 1, 2, now.minusSeconds(60*200));
        long block2 = blocksHelper.insertNumericRangeBlock(taskDefinitionId, 3, 4, now.minusSeconds(60*212));
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*200), now.minusSeconds(60*200), now.minusSeconds(60*201), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*212), now.minusSeconds(60*212), now.minusSeconds(60*211), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.NumericRange,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<RangeBlock> failedBlocks = sut.findFailedRangeBlocks(request);

        // ASSERT
        assertEquals(0, failedBlocks.size());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedListBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*12), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*2), null);
        long block2 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*12), null);
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.List,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> failedBlocks = sut.findFailedListBlocks(request);

        // ASSERT
        assertEquals(1, failedBlocks.size());
        assertEquals(String.valueOf(block1), failedBlocks.get(0).getListBlockId());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedListBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*32), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*2), null);
        long block2 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*12), null);
        long block3 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*22), null);
        long block4 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*32), null);
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block3, now.minusSeconds(60*22), now.minusSeconds(60*22), now.minusSeconds(60*21), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block4, now.minusSeconds(60*32), now.minusSeconds(60*32), now.minusSeconds(60*31), BlockExecutionStatus.Completed, 1);

        int blockCountLimit = 2;

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.List,
                Instant.now().minusSeconds(60*200),
                Instant.now(),
                blockCountLimit,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> failedBlocks = sut.findFailedListBlocks(request);

        // ASSERT
        assertEquals(blockCountLimit, failedBlocks.size());
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getListBlockId().equals(String.valueOf(block2))));
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getListBlockId().equals(String.valueOf(block3))));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedListBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*212), now.minusSeconds(60*200));
        long block1 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*200), null);
        long block2 = blocksHelper.insertListBlock(taskDefinitionId, now.minusSeconds(60*212), null);
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*200), now.minusSeconds(60*200), now.minusSeconds(60*201), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*212), now.minusSeconds(60*212), now.minusSeconds(60*211), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.List,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoListBlock> failedBlocks = sut.findFailedListBlocks(request);

        // ASSERT
        assertEquals(0, failedBlocks.size());
    }


    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedObjectBlocksExistInTargetPeriodAndNumberIsLessThanBlocksLimit_ThenReturnAllFailedBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*12), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*2), UUID.randomUUID().toString());
        long block2 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*12), UUID.randomUUID().toString());
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.Object,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> failedBlocks = sut.findFailedObjectBlocks(request);

        // ASSERT
        assertEquals(1, failedBlocks.size());
        assertEquals(String.valueOf(block1), failedBlocks.get(0).getObjectBlockId());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedObjectBlocksExistInTargetPeriodAndNumberIsGreaterThanBlocksLimit_ThenReturnOldestBlocksUpToCountLimit()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*32), now.minusSeconds(60*1));
        long block1 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*2), UUID.randomUUID().toString());
        long block2 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*12), UUID.randomUUID().toString());
        long block3 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*22), UUID.randomUUID().toString());
        long block4 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*32), UUID.randomUUID().toString());
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*2), now.minusSeconds(60*2), now.minusSeconds(60*1), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*12), now.minusSeconds(60*12), now.minusSeconds(60*11), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block3, now.minusSeconds(60*22), now.minusSeconds(60*22), now.minusSeconds(60*21), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block4, now.minusSeconds(60*32), now.minusSeconds(60*32), now.minusSeconds(60*31), BlockExecutionStatus.Completed, 1);

        int blockCountLimit = 2;

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.Object,
                Instant.now().minusSeconds(60*200),
                Instant.now(),
                blockCountLimit,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> failedBlocks = sut.findFailedObjectBlocks(request);

        // ASSERT
        assertEquals(blockCountLimit, failedBlocks.size());
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(String.valueOf(block2))));
        assertTrue(failedBlocks.stream().anyMatch(x -> x.getObjectBlockId().equals(String.valueOf(block3))));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void When_FailedObjectBlocksExistOutsideTargetPeriod_ThenReturnNoBlocks()
    {
        // ARRANGE
        Instant now = Instant.now();
        String taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId, Duration.ofMinutes(1), now.minusSeconds(60*212), now.minusSeconds(60*200));
        long block1 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*200), UUID.randomUUID().toString());
        long block2 = blocksHelper.insertObjectBlock(taskDefinitionId, now.minusSeconds(60*212), UUID.randomUUID().toString());
        blocksHelper.insertBlockExecution(taskExecution1, block1, now.minusSeconds(60*200), now.minusSeconds(60*200), now.minusSeconds(60*201), BlockExecutionStatus.Failed, 1);
        blocksHelper.insertBlockExecution(taskExecution1, block2, now.minusSeconds(60*212), now.minusSeconds(60*212), now.minusSeconds(60*211), BlockExecutionStatus.Completed, 1);

        FindFailedBlocksRequest request = new FindFailedBlocksRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                "1",
                BlockType.Object,
                Instant.now().minusSeconds(60*20),
                Instant.now(),
                2,
                3);

        // ACT
        BlockRepository sut = createSut();
        List<ProtoObjectBlock> failedBlocks = sut.findFailedObjectBlocks(request);

        // ASSERT
        assertEquals(0, failedBlocks.size());
    }
}
