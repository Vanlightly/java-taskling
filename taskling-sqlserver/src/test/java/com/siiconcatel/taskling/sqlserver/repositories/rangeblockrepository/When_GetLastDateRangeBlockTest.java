package com.siiconcatel.taskling.sqlserver.repositories.rangeblockrepository;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.RangeBlockRepository;
import com.siiconcatel.taskling.sqlserver.blocks.RangeBlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.helpers.BlocksHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import com.siiconcatel.taskling.sqlserver.helpers.TimeHelper;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;
import org.w3c.dom.ranges.Range;

import java.time.Instant;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class When_GetLastDateRangeBlockTest {
    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;

    private int taskDefinitionId;
    private String taskExecution1;
    private Date baseDateTime;

    private String block1;
    private String block2;
    private String block3;
    private String block4;
    private String block5;

    public When_GetLastDateRangeBlockTest()
    {
        blocksHelper = new BlocksHelper();
        blocksHelper.deleteBlocks(TestConstants.ApplicationName);
        executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        TaskRepositoryMsSql.clearCache();
    }

    private RangeBlockRepository createSut()
    {
        return new RangeBlockRepositoryMsSql(new TaskRepositoryMsSql());
    }

    private void insertBlocks()
    {
        taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId);

        baseDateTime = TimeHelper.getDate(2016, 1, 1);
        block1 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId,
                TimeHelper.addMinutes(baseDateTime, -20).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -30).toInstant(),
                Instant.now()));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block1),
                TimeHelper.addMinutes(baseDateTime, -20).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -20).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -25).toInstant(),
                BlockExecutionStatus.Failed,
                1);
        waitFor(10);

        block2 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId,
                TimeHelper.addMinutes(baseDateTime,-10).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-40).toInstant(),
                Instant.now()));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block2),
                TimeHelper.addMinutes(baseDateTime,-30).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-30).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-35).toInstant(),
                BlockExecutionStatus.Started,
                1);
        waitFor(10);

        block3 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId,
                TimeHelper.addMinutes(baseDateTime, -40).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -50).toInstant(),
                Instant.now()));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block3),
                TimeHelper.addMinutes(baseDateTime, -40).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -40).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -45).toInstant(),
                BlockExecutionStatus.NotStarted,
                1);
        waitFor(10);

        block4 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId,
                TimeHelper.addMinutes(baseDateTime,-50).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-60).toInstant(),
                Instant.now()));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block4),
                TimeHelper.addMinutes(baseDateTime,-50).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-50).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-55).toInstant(),
                BlockExecutionStatus.Completed,
                1);
        waitFor(10);

        block5 = String.valueOf(blocksHelper.insertDateRangeBlock(taskDefinitionId,
                TimeHelper.addMinutes(baseDateTime,-60).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-70).toInstant(),
                Instant.now()));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block5),
                TimeHelper.addMinutes(baseDateTime,-60).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-60).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-65).toInstant(),
                BlockExecutionStatus.Started,
                1);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_OrderByLastCreated_ThenReturnLastCreated()
    {
        // ARRANGE
        insertBlocks();

        // ACT
        RangeBlockRepository sut = createSut();
        RangeBlock block = sut.getLastRangeBlock(createRequest(LastBlockOrder.LastCreated));

        // ASSERT
        assertEquals(block5, block.getRangeBlockId());
        assertEquals(TimeHelper.addMinutes(baseDateTime, -60).toInstant(), block.getStartDate());
        assertEquals(TimeHelper.addMinutes(baseDateTime, -70).toInstant(), block.getEndDate());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_OrderByMaxFromDate_ThenReturnBlockWithMaxFromDate()
    {
        // ARRANGE
        insertBlocks();

        // ACT
        RangeBlockRepository sut = createSut();
        RangeBlock block = sut.getLastRangeBlock(createRequest(LastBlockOrder.RangeStart));

        // ASSERT
        assertEquals(block2, block.getRangeBlockId());
        assertEquals(TimeHelper.addMinutes(baseDateTime,-10).toInstant(), block.getStartDate());
        assertEquals(TimeHelper.addMinutes(baseDateTime,-40).toInstant(), block.getEndDate());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_OrderByMaxToDate_ThenReturnBlockWithMaxToDate()
    {
        // ARRANGE
        insertBlocks();

        // ACT
        RangeBlockRepository sut = createSut();
        RangeBlock block = sut.getLastRangeBlock(createRequest(LastBlockOrder.RangeEnd));

        // ASSERT
        assertEquals(block1, block.getRangeBlockId());
        assertEquals(TimeHelper.addMinutes(baseDateTime,-20).toInstant(), block.getStartDate());
        assertEquals(TimeHelper.addMinutes(baseDateTime,-30).toInstant(), block.getEndDate());
    }

    private LastBlockRequest createRequest(LastBlockOrder lastBlockOrder)
    {
        LastBlockRequest request = new LastBlockRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), BlockType.DateRange);
        request.setLastBlockOrder(lastBlockOrder);

        return request;
    }

    private void waitFor(int milliseconds)
    {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {}
    }
}
