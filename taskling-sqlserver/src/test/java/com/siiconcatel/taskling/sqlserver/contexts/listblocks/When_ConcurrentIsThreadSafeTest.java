package com.siiconcatel.taskling.sqlserver.contexts.listblocks;

import com.siiconcatel.taskling.core.blocks.listblocks.BatchSize;
import com.siiconcatel.taskling.core.blocks.listblocks.ItemStatus;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockItem;
import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.SlowTests;
import com.siiconcatel.taskling.sqlserver.contexts.PersonDto;
import com.siiconcatel.taskling.sqlserver.helpers.BlocksHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ClientHelper;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.helpers.TestConstants;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;

import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class When_ConcurrentIsThreadSafeTest {

    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;

    public When_ConcurrentIsThreadSafeTest()
    {
        blocksHelper = new BlocksHelper();
        blocksHelper.deleteBlocks(TestConstants.ApplicationName);
        executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        int taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertUnlimitedExecutionToken(taskDefinitionId);
    }

    @Category({SlowTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithSingleUnitCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        // ACT and // ASSERT
        boolean startedOk;
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000));
        startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getList(100000);
            short maxBlockSize = 1000;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class, x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts();
            for(ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                List<ListBlockItem<PersonDto>> items = listBlock.getItems(ItemStatus.Failed, ItemStatus.Pending);
                items.parallelStream().forEach((currentItem) -> {
                    listBlock.itemComplete(currentItem);
                });

                listBlock.complete();

                // All items should be completed now
                assertEquals((listBlock.getItems(ItemStatus.Completed)).size(), blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
            }
        }
        executionContext.complete();
    }

    @Category({SlowTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithBatchCommitAtEnd_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        // ACT and // ASSERT
        boolean startedOk;
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000));
        startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getList(100000);
            short maxBlockSize = 1000;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class, x -> x.withBatchCommitAtEnd(values, maxBlockSize)).getBlockContexts();
            for(ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                List<ListBlockItem<PersonDto>> items = listBlock.getItems(ItemStatus.Failed, ItemStatus.Pending);
                items.parallelStream().forEach((currentItem) -> {
                    listBlock.itemComplete(currentItem);
                });

                listBlock.complete();

                // All items should be completed now
                assertEquals((listBlock.getItems(ItemStatus.Completed)).size(), blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
            }
        }
        executionContext.complete();
    }

    @Category({SlowTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithPeriodicCommit_BlocksProcessedSequentially_BlocksListItemsProcessedInParallel_ThenNoConcurrencyIssues()
    {
        boolean startedOk;
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000));
        startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getList(100000);
            short maxBlockSize = 1000;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class, x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Hundred)).getBlockContexts();
            for(ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                List<ListBlockItem<PersonDto>> items = listBlock.getItems(ItemStatus.Failed, ItemStatus.Pending);
                items.parallelStream().forEach((currentItem) -> {
                    listBlock.itemComplete(currentItem);
                });

                listBlock.complete();

                // All items should be completed now
                assertEquals((listBlock.getItems(ItemStatus.Completed)).size(), blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
            }
        }
        executionContext.complete();
    }

    @Category({SlowTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithSingleUnitCommit_BlocksProcessedInParallel_BlocksListItemsProcessedSequentially_ThenNoConcurrencyIssues()
    {
        // ACT and // ASSERT
        boolean startedOk;
        TaskExecutionContext executionContext = ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(10000));
        startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getList(100000);
            short maxBlockSize = 1000;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class, x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts();
            listBlocks.parallelStream().forEach((listBlock) -> {
                listBlock.start();
                List<ListBlockItem<PersonDto>> items = listBlock.getItems(ItemStatus.Failed, ItemStatus.Pending);
                for(ListBlockItem<PersonDto> currentItem : items)
                    listBlock.itemComplete(currentItem);

                listBlock.complete();

                // All items should be completed now
                assertEquals((listBlock.getItems(ItemStatus.Completed)).size(), blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
            });
        }
        executionContext.complete();
    }

    private List<PersonDto> getList(int count)
    {
        List<PersonDto> list = new ArrayList<>();

        for (int i = 0; i < count; i++)
        {
            list.add(new PersonDto(String.valueOf(i), "Tito" + i, Date.from(Instant.now())));
        }

        return list;
    }
}
