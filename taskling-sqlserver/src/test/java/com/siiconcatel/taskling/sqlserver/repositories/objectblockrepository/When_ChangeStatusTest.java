package com.siiconcatel.taskling.sqlserver.repositories.objectblockrepository;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ObjectBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;
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
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class When_ChangeStatusTest {
    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;

    private int taskDefinitionId;
    private String taskExecution1;
    private Date baseDateTime;
    private long blockExecutionId;

    public When_ChangeStatusTest()
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

    private void insertObjectBlock()
    {
        taskExecution1 = executionHelper.insertOverrideTaskExecution(taskDefinitionId);

        baseDateTime = TimeHelper.getDate(2016, 1, 1);
        String block1 = String.valueOf(blocksHelper.insertObjectBlock(taskDefinitionId, Instant.now(), UUID.randomUUID().toString()));
        blockExecutionId = blocksHelper.insertBlockExecution(taskExecution1, Long.parseLong(block1),
                TimeHelper.addMinutes(baseDateTime,-20).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-20).toInstant(),
                TimeHelper.addMinutes(baseDateTime,-25).toInstant(),
                BlockExecutionStatus.Started,
                1);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_SetStatusOfObjectBlock_ThenItemsCountIsCorrect()
    {
        // ARRANGE
        insertObjectBlock();

        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(
                new TaskId(TestConstants.ApplicationName, TestConstants.TaskName),
                taskExecution1,
                BlockType.Object,
                String.valueOf(blockExecutionId),
                BlockExecutionStatus.Completed);
        request.setItemsProcessed(10000);


        // ACT
        ObjectBlockRepository sut = createSut();
        sut.changeStatus(request);

        int itemCount = new BlocksHelper().getBlockExecutionItemCount(blockExecutionId);

        // ASSERT
        assertEquals(10000, itemCount);
    }
}
