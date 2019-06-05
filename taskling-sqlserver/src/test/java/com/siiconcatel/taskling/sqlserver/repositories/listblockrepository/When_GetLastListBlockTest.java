package com.siiconcatel.taskling.sqlserver.repositories.listblockrepository;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.core.serde.TasklingSerde;
import com.siiconcatel.taskling.sqlserver.blocks.ListBlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.CriticalSectionTokens;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.helpers.BlocksHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import com.siiconcatel.taskling.sqlserver.helpers.TimeHelper;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class When_GetLastListBlockTest {
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

    public When_GetLastListBlockTest()
    {
        blocksHelper = new BlocksHelper();
        blocksHelper.deleteBlocks(TestConstants.ApplicationName);
        executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        TaskRepositoryMsSql.clearCache();
    }

    private ListBlockRepository createSut()
    {
        return new ListBlockRepositoryMsSql(new TaskRepositoryMsSql());
    }

    private void insertBlocks()
    {
        taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId);

        baseDateTime = TimeHelper.getDate(2016, 1, 1);
        DateRange dateRange1 = new DateRange(TimeHelper.addMinutes(baseDateTime, -20), baseDateTime);
        block1 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, Instant.now(), TasklingSerde.serialize(dateRange1, false)));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block1),
                TimeHelper.addMinutes(baseDateTime, -20).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -20).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -25).toInstant(),
                BlockExecutionStatus.Failed,
                1);

        waitFor(10);
        DateRange dateRange2 = new DateRange(TimeHelper.addMinutes(baseDateTime, -30), baseDateTime);
        block2 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, Instant.now(), TasklingSerde.serialize(dateRange2, false)));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block2),
                TimeHelper.addMinutes(baseDateTime, -30).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -30).toInstant(),
                TimeHelper.addMinutes(baseDateTime, -35).toInstant(),
                BlockExecutionStatus.Started,
                1);

        waitFor(10);
        DateRange dateRange3 = new DateRange(TimeHelper.addMinutes(baseDateTime,-40), baseDateTime);
        block3 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, Instant.now(), TasklingSerde.serialize(dateRange3, false)));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block3),
                TimeHelper.addMinutes(baseDateTime,-40).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-40).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-45).toInstant(),
                BlockExecutionStatus.NotStarted,
                1);

        waitFor(10);
        DateRange dateRange4 = new DateRange(TimeHelper.addMinutes(baseDateTime,-50), baseDateTime);
        block4 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, Instant.now(), TasklingSerde.serialize(dateRange4, false)));
        blocksHelper.insertBlockExecution(taskExecution1,
                Long.parseLong(block4),
                TimeHelper.addMinutes(baseDateTime,-50).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-50).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-55).toInstant(),
                BlockExecutionStatus.Completed,
                1);

        waitFor(10);
        DateRange dateRange5 = new DateRange(TimeHelper.addMinutes(baseDateTime,-60), baseDateTime);
        block5 = String.valueOf(blocksHelper.insertListBlock(taskDefinitionId, Instant.now(), TasklingSerde.serialize(dateRange5, false)));
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
    public void ThenReturnLastCreated()
    {
        // ARRANGE
        insertBlocks();

        // ACT
        ListBlockRepository sut = createSut();
        ProtoListBlock block = sut.getLastListBlock(createRequest());

        DateRange header = TasklingSerde.deserialize(DateRange.class, block.getHeader(), false);

        // ASSERT
        assertEquals(block5, block.getListBlockId());
        assertEquals(TimeHelper.addMinutes(TimeHelper.getDate(2016, 1, 1),-60), header.getFromDate());
        assertEquals(TimeHelper.getDate(2016, 1, 1), header.getToDate());
    }

    private LastBlockRequest createRequest()
    {
        LastBlockRequest request = new LastBlockRequest(new TaskId(TestConstants.ApplicationName, TestConstants.TaskName), BlockType.Object);
        request.setLastBlockOrder(LastBlockOrder.LastCreated);

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
