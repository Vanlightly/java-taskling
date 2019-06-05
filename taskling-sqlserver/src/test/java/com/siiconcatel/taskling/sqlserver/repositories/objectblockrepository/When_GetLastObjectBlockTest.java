package com.siiconcatel.taskling.sqlserver.repositories.objectblockrepository;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ObjectBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;
import com.siiconcatel.taskling.core.serde.TasklingSerde;
import com.siiconcatel.taskling.sqlserver.blocks.ObjectBlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.helpers.BlocksHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import com.siiconcatel.taskling.sqlserver.helpers.TimeHelper;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class When_GetLastObjectBlockTest {
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

    public When_GetLastObjectBlockTest()
    {
        blocksHelper = new BlocksHelper();
        blocksHelper.deleteBlocks(TestConstants.ApplicationName);
        executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);

        TaskRepositoryMsSql.clearCache();
    }

    private ObjectBlockRepository createSut()
    {
        return new ObjectBlockRepositoryMsSql(new TaskRepositoryMsSql());
    }

    private void insertBlocks()
    {
        taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId);

        baseDateTime = TimeHelper.getDate(2016, 1, 1);
        block1 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, Instant.now(), "Testing1"));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block1), TimeHelper.addMinutes(baseDateTime,-20).toInstant(), TimeHelper.addMinutes(baseDateTime,-20).toInstant(), TimeHelper.addMinutes(baseDateTime, -25).toInstant(), BlockExecutionStatus.Failed, 1);
        waitFor(10);
        block2 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, Instant.now(), "Testing2"));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block2), TimeHelper.addMinutes(baseDateTime,-30).toInstant(), TimeHelper.addMinutes(baseDateTime,-30).toInstant(), TimeHelper.addMinutes(baseDateTime, -35).toInstant(), BlockExecutionStatus.Started, 1);
        waitFor(10);
        block3 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, Instant.now(), "Testing3"));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block3), TimeHelper.addMinutes(baseDateTime,-40).toInstant(), TimeHelper.addMinutes(baseDateTime,-40).toInstant(), TimeHelper.addMinutes(baseDateTime, -45).toInstant(), BlockExecutionStatus.NotStarted, 1);
        waitFor(10);
        block4 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, Instant.now(), "Testing4"));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block4), TimeHelper.addMinutes(baseDateTime,-50).toInstant(), TimeHelper.addMinutes(baseDateTime,-50).toInstant(), TimeHelper.addMinutes(baseDateTime, -55).toInstant(), BlockExecutionStatus.Completed, 1);
        waitFor(10);
        block5 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, Instant.now(), "Testing5"));
        blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block5), TimeHelper.addMinutes(baseDateTime,-60).toInstant(), TimeHelper.addMinutes(baseDateTime,-60).toInstant(), TimeHelper.addMinutes(baseDateTime, -65).toInstant(), BlockExecutionStatus.Started, 1);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void thenReturnLastCreated()
    {
        // ARRANGE
        insertBlocks();

        // ACT
        ObjectBlockRepository sut = createSut();
        ProtoObjectBlock block = sut.getLastObjectBlock(createRequest());

        // ASSERT
        assertEquals(block5, block.getObjectBlockId());
        assertEquals("Testing5", TasklingSerde.deserialize(String.class, block.getObjectData(), false));
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
