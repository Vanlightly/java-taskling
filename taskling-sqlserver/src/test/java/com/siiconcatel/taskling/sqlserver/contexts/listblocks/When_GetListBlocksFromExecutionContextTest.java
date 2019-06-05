package com.siiconcatel.taskling.sqlserver.contexts.listblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.listblocks.*;
import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.contexts.PersonDto;
import com.siiconcatel.taskling.sqlserver.helpers.*;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class When_GetListBlocksFromExecutionContextTest {

    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;
    private int taskDefinitionId = 0;
    private int MaxBlocks = 10000;

    public When_GetListBlocksFromExecutionContextTest()
    {
        blocksHelper = new BlocksHelper();
        blocksHelper.deleteBlocks(TestConstants.ApplicationName);
        executionHelper = new ExecutionsHelper();
        executionHelper.deleteRecordsOfApplication(TestConstants.ApplicationName);

        taskDefinitionId = executionHelper.insertTask(TestConstants.ApplicationName, TestConstants.TaskName);
        executionHelper.insertAvailableExecutionToken(taskDefinitionId, 1);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithSingleUnitCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        // ARRANGE

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(9, 0);
            short maxBlockSize = 4;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts();
            // There should be 3 blocks - 4, 4, 1
            assertEquals(3, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
            int expectedNotStartedCount = 3;
            int expectedCompletedCount = 0;

            // All three should be registered as not started
            assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
            assertEquals(0, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
            assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                expectedNotStartedCount--;

                // There should be one less NotStarted block and exactly 1 Started block
                assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                assertEquals(1, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                int expectedCompletedItems = 0;
                int expectedPendingItems = listBlock.getItems(ItemStatus.Pending).size();
                // All items should be Pending and 0 Completed
                assertEquals(expectedPendingItems, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Pending));
                assertEquals(expectedCompletedItems, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
                for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                {
                    // do the processing

                    itemToProcess.completed();

                    // More more should be Completed
                    expectedCompletedItems++;
                    assertEquals(expectedCompletedItems, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
                }

                listBlock.complete();

                // One more block should be completed
                expectedCompletedCount++;
                assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
            }
        }

        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithSingleUnitCommitAndFailsWithReason_ThenReasonIsPersisted()
    {
        // ARRANGE

        // ACT and // ASSERT
        String listBlockId = "";
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(9, 0);
            short maxBlockSize = 4;
            ListBlockContext<PersonDto> listBlock = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts().get(0);
            listBlockId = listBlock.getBlock().getListBlockId();
            listBlock.start();

            int counter = 0;
            for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
            {
                itemToProcess.failed("Exception");

                counter++;
            }

            listBlock.complete();
        }

        assertTrue(blocksHelper.getListBlockItems(PersonDto.class, listBlockId, ItemStatus.Failed)
                .stream()
                .allMatch(x -> x.getStatusReason().equals("Exception")));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_LargeValues_ThenValuesArePersistedAndRetrievedOk()
    {
        // ARRANGE
        List<PersonDto> values = getLargePersonList(4, 0);

        // ACT and // ASSERT
        String listBlockId = "";
        TaskExecutionContext executionContext = createTaskExecutionContext(1);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {

            short maxBlockSize = 4;
            ListBlockContext<PersonDto> listBlock = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts().get(0);
            listBlockId = listBlock.getBlock().getListBlockId();
            listBlock.start();

            for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                itemToProcess.failed("Exception");

            listBlock.complete();
        }
        executionContext.complete();

        TaskExecutionContext executionContext2 = createTaskExecutionContext(1);
        startedOk = executionContext2.tryStart();
        if (startedOk)
        {
            List<PersonDto> emptyPersonList = new ArrayList<>();
            short maxBlockSize = 4;
            ListBlockContext<PersonDto> listBlock = executionContext2.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(emptyPersonList, maxBlockSize)).getBlockContexts().get(0);
            listBlockId = listBlock.getBlock().getListBlockId();
            listBlock.start();

            List<ListBlockItem<PersonDto>> itemsToProcess = listBlock.getItems(ItemStatus.Pending, ItemStatus.Failed);
            for (int i = 0; i < itemsToProcess.size(); i++)
            {
                assertEquals(values.get(i).getDateOfBirth(), itemsToProcess.get(i).getValue().getDateOfBirth());
                assertEquals(values.get(i).getId(), itemsToProcess.get(i).getValue().getId());
                assertEquals(values.get(i).getName(), itemsToProcess.get(i).getValue().getName());
            }

            listBlock.complete();
        }
        executionContext2.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithNoValues_ThenCheckpointIsPersistedAndEmptyBlockGenerated()
    {
        // ARRANGE

        // ACT and // ASSERT
        String listBlockId = "";
        TaskExecutionContext executionContext = createTaskExecutionContext(1);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = new ArrayList<>();
            short maxBlockSize = 4;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts();
            assertTrue(listBlocks.isEmpty());
            LastEvent execEvent = executionHelper.getLastEvent(taskDefinitionId);
            assertEquals(EventType.CheckPoint, execEvent.Type);
            assertEquals("No values for generate the block. Emtpy Block context returned.", execEvent.Description);
        }
        executionContext.complete();
    }

//
//    @Category({FastTests.class, BlocksTests.class})
//    @Test
//    public void If_AsListWithSingleUnitCommitAndStepSet_ThenStepIsPersisted()
//    {
//        // ARRANGE
//
//        // ACT and // ASSERT
//        String listBlockId = "";
//        TaskExecutionContext executionContext = createTaskExecutionContext(1);
//        boolean startedOk = await executionContext.tryStart();
//        if (startedOk)
//        {
//            List<PersonDto> values = getPersonList(9, 0);
//            short maxBlockSize = 4;
//            ListBlockContext<PersonDto> listBlock = executionContext.getListBlocks(PersonDto.class, x -> x.withSingleUnitCommit(values, maxBlockSize)).get(0);
//            listBlockId = listBlock.getBlock().getListBlockId();
//            listBlock.start();
//
//            int counter = 0;
//            for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
//            {
//                await itemToProcess.FailedAsync("Exception");
//
//                counter++;
//            }
//
//            await listBlock.CompleteAsync();
//        }
//
//        Assert.True(_blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed).All(x => x.StatusReason == "Exception" && x.Step == 2));
//    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithBatchCommitAtEnd_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        // ARRANGE

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(9, 0);
            short maxBlockSize = 4;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withBatchCommitAtEnd(values, maxBlockSize)).getBlockContexts();
            // There should be 3 blocks - 4, 4, 1
            assertEquals(3, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
            int expectedNotStartedCount = 3;
            int expectedCompletedCount = 0;

            // All three should be registered as not started
            assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
            assertEquals(0, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
            assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                expectedNotStartedCount--;

                // There should be one less NotStarted block and exactly 1 Started block
                assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                assertEquals(1, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                int expectedPendingItems = listBlock.getItems(ItemStatus.Pending).size();
                // All items should be Pending and 0 Completed
                assertEquals(expectedPendingItems, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Pending));
                assertEquals(0, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));

                for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                {
                    // do the processing

                    itemToProcess.completed();

                    // There should be 0 Completed because we batch commit at the end
                    assertEquals(0, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
                }

                listBlock.complete();

                // All items should be completed now
                assertEquals(listBlock.getItems(ItemStatus.Completed).size(), blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));

                // One more block should be completed
                expectedCompletedCount++;
                assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
            }
        }

        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithPeriodicCommit_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        // ARRANGE

        // ACT and // ASSERT

        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(26, 0);
            short maxBlockSize = 15;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).getBlockContexts();
            // There should be 2 blocks - 15, 11
            assertEquals(2, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
            int expectedNotStartedCount = 2;
            int expectedCompletedCount = 0;

            // All three should be registered as not started
            assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
            assertEquals(0, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
            assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                expectedNotStartedCount--;

                // There should be one less NotStarted block and exactly 1 Started block
                assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                assertEquals(1, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                int expectedPendingItems = listBlock.getItems(ItemStatus.Pending).size();
                int expectedCompletedItems = 0;
                // All items should be Pending and 0 Completed
                assertEquals(expectedPendingItems, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Pending));
                assertEquals(expectedCompletedItems, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
                int itemsProcessed = 0;
                int itemsCommitted = 0;

                for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                {
                    itemsProcessed++;
                    // do the processing

                    itemToProcess.completed();

                    // There should be 0 Completed unless we have reached the batch size 10
                    if (itemsProcessed % 10 == 0)
                    {
                        assertEquals(10, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
                        itemsCommitted += 10;
                    }
                    else
                        assertEquals(itemsCommitted, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));
                }


                listBlock.complete();

                // All items should be completed now
                assertEquals(itemsProcessed, blocksHelper.getListBlockItemCountByStatus(listBlock.getListBlockId(), ItemStatus.Completed));

                // One more block should be completed
                expectedCompletedCount++;
                assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
            }
        }

        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithPeriodicCommitAndFailsWithReason_ThenReasonIsPersisted()
    {
        // ARRANGE

        // ACT and // ASSERT
        String listBlockId = "";
        TaskExecutionContext executionContext = createTaskExecutionContext(1);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(14, 0);
            short maxBlockSize = 20;
            ListBlockContext<PersonDto> listBlock = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).getBlockContexts().get(0);
            listBlockId = listBlock.getBlock().getListBlockId();
            listBlock.start();

            int counter = 0;
            for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
            {
                itemToProcess.failed("Exception");

                counter++;
            }

            listBlock.complete();
        }

        executionContext.complete();

        assertTrue(blocksHelper.getListBlockItems(PersonDto.class, listBlockId, ItemStatus.Failed)
                .stream()
                .allMatch(x -> x.getStatusReason().equals("Exception")));
    }

//    @Category({FastTests.class, BlocksTests.class})
//    @Test
//    public void If_AsListWithPeriodicCommitAndStepSet_ThenStepIsPersisted()
//    {
//        // ARRANGE
//
//        // ACT and // ASSERT
//        bool startedOk;
//        string listBlockId = string.Empty;
//        using (var executionContext = CreateTaskExecutionContext(1))
//        {
//            startedOk = await executionContext.TryStartAsync();
//            if (startedOk)
//            {
//                var values = GetPersonList(14);
//                short maxBlockSize = 20;
//                var listBlock = (await executionContext.GetListBlocksAsync<PersonDto>(x => x.WithPeriodicCommit(values, maxBlockSize, BatchSize.Ten))).First();
//                listBlockId = listBlock.Block.ListBlockId;
//                await listBlock.StartAsync();
//
//                int counter = 0;
//                foreach (var itemToProcess in await listBlock.GetItemsAsync(ItemStatus.Pending))
//                {
//                    itemToProcess.Step = 2;
//                    await itemToProcess.FailedAsync("Exception");
//
//                    counter++;
//                }
//
//                await listBlock.CompleteAsync();
//            }
//        }
//
//        var listBlockItems = _blocksHelper.GetListBlockItems<PersonDto>(listBlockId, ItemStatus.Failed);
//        Assert.True(listBlockItems.All(x => x.StatusReason == "Exception" && x.Step == 2));
//    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_PreviousBlock_ThenLastBlockContainsCorrectItems()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(26, 0);
            short maxBlockSize = 15;
            List<ListBlockContext<PersonDto>> listBlocks = prev.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).getBlockContexts();

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                    itemToProcess.completed();

                listBlock.complete();
            }
        }
        prev.complete();

        List<PersonDto> expectedPeople = getPersonList(11, 15);
        ListBlock<PersonDto> expectedLastBlock = new ListBlockImpl<PersonDto>();
        for (PersonDto person : expectedPeople)
            expectedLastBlock.getItems().add(new ListBlockItemImpl<PersonDto>("", person, ItemStatus.Pending, "", Instant.now()));

        // ACT
        ListBlock<PersonDto> lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        if (startedOk)
            lastBlock = executionContext.getLastListBlock(PersonDto.class);
        executionContext.complete();

        // ASSERT
        List<ListBlockItem<PersonDto>> expectedItems = expectedLastBlock.getItems();
        List<ListBlockItem<PersonDto>> lastBlockItems = lastBlock.getItems();
        assertEquals(expectedItems.size(), lastBlockItems.size());
        assertEquals(expectedItems.get(0).getValue().getId(), lastBlockItems.get(0).getValue().getId());
        assertEquals(expectedItems.get(1).getValue().getId(), lastBlockItems.get(1).getValue().getId());
        assertEquals(expectedItems.get(2).getValue().getId(), lastBlockItems.get(2).getValue().getId());
        assertEquals(expectedItems.get(3).getValue().getId(), lastBlockItems.get(3).getValue().getId());
        assertEquals(expectedItems.get(4).getValue().getId(), lastBlockItems.get(4).getValue().getId());
        assertEquals(expectedItems.get(5).getValue().getId(), lastBlockItems.get(5).getValue().getId());
        assertEquals(expectedItems.get(6).getValue().getId(), lastBlockItems.get(6).getValue().getId());
        assertEquals(expectedItems.get(7).getValue().getId(), lastBlockItems.get(7).getValue().getId());
        assertEquals(expectedItems.get(8).getValue().getId(), lastBlockItems.get(8).getValue().getId());
        assertEquals(expectedItems.get(9).getValue().getId(), lastBlockItems.get(9).getValue().getId());
        assertEquals(expectedItems.get(10).getValue().getId(), lastBlockItems.get(10).getValue().getId());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_NoPreviousBlock_ThenLastBlockIsNull()
    {
        // ARRANGE
        // all previous blocks were deleted in TestInitialize

        // ACT
        ListBlock<PersonDto> lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            lastBlock = executionContext.getLastListBlock(PersonDto.class);
        }

        // ASSERT
        assertNull(lastBlock);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_PreviousBlockIsPhantom_ThenLastBlockNotThisPhantom()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(3,0);
            short maxBlockSize = 15;
            List<ListBlockContext<PersonDto>> listBlocks = prev.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).getBlockContexts();

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                    itemToProcess.completed();

                listBlock.complete();
            }
        }
        prev.complete();

        blocksHelper.insertPhantomListBlock(TestConstants.ApplicationName, TestConstants.TaskName);

        // ACT

        ListBlock<PersonDto> lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        if (startedOk)
        {
            lastBlock = executionContext.getLastListBlock(PersonDto.class);
        }
        executionContext.complete();

        // ASSERT
        List<ListBlockItem<PersonDto>> lastBlockItems = lastBlock.getItems();
        assertEquals(3, lastBlockItems.size());
        assertEquals("1", lastBlockItems.get(0).getValue().getId());
        assertEquals("2", lastBlockItems.get(1).getValue().getId());
        assertEquals("3", lastBlockItems.get(2).getValue().getId());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AnItemFails_ThenCompleteSetsStatusAsFailed()
    {
        // ARRANGE

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(1);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(9, 0);
            short maxBlockSize = 4;
            ListBlockContext<PersonDto> listBlock = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts().get(0);

            listBlock.start();

            int counter = 0;
            for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
            {
                if (counter == 2)
                    itemToProcess.failed("Exception");
                else
                    itemToProcess.completed();

                counter++;
            }

            listBlock.complete();
        }
        executionContext.complete();

        assertEquals(1, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Failed));
        assertEquals(0, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_ReprocessingSpecificExecutionAndItExistsWithMultipleExecutionsAndOnlyOneFailed_ThenBringBackOnFailedBlockWhenRequested()
    {
        String referenceValue = UUID.randomUUID().toString();
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart(referenceValue);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(9, 0);
            short maxBlockSize = 3;
            List<ListBlockContext<PersonDto>> listBlocks = prev.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Fifty)).getBlockContexts();

            // block 0 has one failed item
            listBlocks.get(0).start();

            int counter = 0;
            for (ListBlockItem<PersonDto> itemToProcess : listBlocks.get(0).getItems(ItemStatus.Pending))
            {
                if (counter == 2)
                    listBlocks.get(0).itemFailed(itemToProcess, "Exception");
                else
                    listBlocks.get(0).itemComplete(itemToProcess);

                counter++;
            }

            listBlocks.get(0).complete();

            // block 1 succeeds
            listBlocks.get(1).start();

            for (ListBlockItem<PersonDto> itemToProcess : listBlocks.get(1).getItems(ItemStatus.Pending))
            {
                listBlocks.get(1).itemComplete(itemToProcess);

                counter++;
            }

            listBlocks.get(1).complete();

            // block 2 never starts
        }
        prev.complete();

        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<ListBlockContext<PersonDto>> listBlocksToReprocess = executionContext.getListBlocks(PersonDto.class,
                    x -> x.reprocessWithPeriodicCommit(BatchSize.Fifty)
                        .pendingAndFailedBlocks()
                        .ofExecutionWith(referenceValue)).getBlockContexts();

            // one failed and one block never started
            assertEquals(2, listBlocksToReprocess.size());

            // the block that failed has one failed item
            List<ListBlockItem<PersonDto>> itemsOfB1 = listBlocksToReprocess.get(0).getItems(ItemStatus.Failed, ItemStatus.Pending);
            assertEquals(1, itemsOfB1.size());
            assertEquals("Exception", itemsOfB1.get(0).getStatusReason());

            // the block that never executed has 3 pending items
            List<ListBlockItem<PersonDto>> itemsOfB2 = listBlocksToReprocess.get(1).getItems(ItemStatus.Failed, ItemStatus.Pending);
            assertEquals(3, itemsOfB2.size());

            listBlocksToReprocess.get(0).complete();
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithOverridenConfiguration_ThenOverridenValuesAreUsed()
    {
        // ARRANGE
        createFailedTask();
        createDeadTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(8, 0);
            short maxBlockSize = 4;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class, x -> x.withSingleUnitCommit(values, maxBlockSize)
                .overrideConfiguration()
                .reprocessFailedTasks(Duration.ofDays(1), (short)3)
                .reprocessDeadTasks(Duration.ofDays(1), (short)3)
                .maximumBlocksToGenerate(5)).getBlockContexts();

            // There should be 5 blocks - 3, 3, 3, 3, 4
            assertEquals(5, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
            assertTrue(listBlocks.get(0).getItems().stream().allMatch(x -> x.getStatus() == ItemStatus.Failed));
            assertEquals(3, listBlocks.get(0).getItems().size());
            assertTrue(listBlocks.get(1).getItems().stream().allMatch(x -> x.getStatus() == ItemStatus.Failed));
            assertEquals(3, listBlocks.get(1).getItems().size());
            assertTrue(listBlocks.get(2).getItems().stream().allMatch(x -> x.getStatus() == ItemStatus.Pending));
            assertEquals(3, listBlocks.get(2).getItems().size());
            assertTrue(listBlocks.get(3).getItems().stream().allMatch(x -> x.getStatus() == ItemStatus.Pending));
            assertEquals(3, listBlocks.get(3).getItems().size());
            assertTrue(listBlocks.get(4).getItems().stream().allMatch(x -> x.getStatus() == ItemStatus.Pending));
            assertEquals(4, listBlocks.get(4).getItems().size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsListWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        // ARRANGE
        createFailedTask();
        createDeadTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(8, 0);
            short maxBlockSize = 4;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts();
            // There should be 2 blocks - 4, 4
            assertEquals(2, listBlocks.size());
            assertTrue(listBlocks.get(0).getItems().stream().allMatch(x -> x.getStatus() == ItemStatus.Pending));
            assertEquals(4, listBlocks.get(0).getItems().size());
            assertTrue(listBlocks.get(1).getItems().stream().allMatch(x -> x.getStatus() == ItemStatus.Pending));
            assertEquals(4, listBlocks.get(1).getItems().size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsList_ThenReturnsBlockInOrderOfBlockId()
    {
        // ARRANGE

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(10, 0);
            short maxBlockSize = 1;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts();

            int counter = 0;
            int lastId = 0;
            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();

                int currentId = Integer.parseInt(listBlock.getBlock().getListBlockId());
                if (counter > 0)
                    assertEquals(currentId, lastId + 1);

                lastId = currentId;

                listBlock.complete();
            }
        }

        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
    {
        // ARRANGE
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(3, 0);
            short maxBlockSize = 15;
            List<ListBlockContext<PersonDto>> listBlocks = prev.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).getBlockContexts();

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                    listBlock.itemComplete(itemToProcess);

                listBlock.complete();
            }
        }
        prev.complete();

        // add this processed block to the forced queue
        long lastBlockId = blocksHelper.getLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
        blocksHelper.enqueueForcedBlock(lastBlockId);

        // ACT - reprocess the forced block
        TaskExecutionContext reprocContext = createTaskExecutionContext(MaxBlocks);
        startedOk = reprocContext.tryStart();
        if (startedOk)
        {
            List<ListBlockContext<PersonDto>> listBlocks = reprocContext.getListBlocks(PersonDto.class,
                    x -> x.withBatchCommitAtEnd(new ArrayList<PersonDto>(), (short)10)).getBlockContexts();
            assertEquals(1, listBlocks.size());

            List<ListBlockItem<PersonDto>> items = listBlocks.get(0).getItems();
            assertEquals(3, items.size());
            assertEquals("1", items.get(0).getValue().getId());
            assertEquals("2", items.get(1).getValue().getId());
            assertEquals("3", items.get(2).getValue().getId());

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();

                for (ListBlockItem<PersonDto> item : listBlock.getItems())
                    listBlock.itemComplete(item);

                listBlock.complete();
            }
        }
        reprocContext.complete();

        // The forced block will have been dequeued so it should not be processed again
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> items = new ArrayList<>();
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(items, (short)50)).getBlockContexts();
            assertEquals(0, listBlocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_BlockItemsAccessedBeforeGetItemsCalled_ThenItemsAreLoadedOkAnyway()
    {
        // ARRANGE

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(10, 0);
            short maxBlockSize = 1;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize)).getBlockContexts();

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();

                List<ListBlockItem<PersonDto>> itemsToProcess = listBlock.getBlock().getItems();
                for (ListBlockItem<PersonDto> item : itemsToProcess)
                    item.completed();

                listBlock.complete();
            }
        }
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_IfMoreItemsThanMaxBlocksAllows_ThenResponseIndicatesNotAllItemsIncluded()
    {
        // ARRANGE

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(2);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(7, 0);
            short maxBlockSize = 2;
            ListBlockResponse<PersonDto> listBlockResponse = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withSingleUnitCommit(values, maxBlockSize));
            assertFalse(listBlockResponse.allItemsIncluded());
            assertEquals(3, listBlockResponse.getExcludedItems().size());
            assertEquals("5", listBlockResponse.getExcludedItems().get(0).getId());
            assertEquals("6", listBlockResponse.getExcludedItems().get(1).getId());
            assertEquals("7", listBlockResponse.getExcludedItems().get(2).getId());
        }
        executionContext.complete();
    }

    private void createFailedTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(6, 0);
            short maxBlockSize = 3;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).getBlockContexts();

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
                for (ListBlockItem<PersonDto> itemToProcess : listBlock.getItems(ItemStatus.Pending))
                    listBlock.itemFailed(itemToProcess, "Exception");

                listBlock.failed("Something bad happened");
            }
        }
        else {
            throw new RuntimeException("Could not start failed task");
        }

        executionContext.complete();
    }

    private void createDeadTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<PersonDto> values = getPersonList(6, 0);
            short maxBlockSize = 3;
            List<ListBlockContext<PersonDto>> listBlocks = executionContext.getListBlocks(PersonDto.class,
                    x -> x.withPeriodicCommit(values, maxBlockSize, BatchSize.Ten)).getBlockContexts();

            for (ListBlockContext<PersonDto> listBlock : listBlocks)
            {
                listBlock.start();
            }
        }
        else {
            throw new RuntimeException("Could not start dead task");
        }
        executionContext.complete();

        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.setLastExecutionAsDead(taskDefinitionId);
    }

    private TaskExecutionContext createTaskExecutionContext(int maxBlocksToGenerate)
    {
        return ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocksToGenerate));
    }

    private TaskExecutionContext createTaskExecutionContextWithNoReprocessing()
    {
        return ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(MaxBlocks));
    }

    private List<PersonDto> getPersonList(int count, int skip)
    {
        List<PersonDto> people = new ArrayList<>();
        people.add(new PersonDto("1", "Boris", new Date(1980, 1, 1)));
        people.add(new PersonDto("2", "Bob", new Date(1981, 1, 1)));
        people.add(new PersonDto("3", "Jane", new Date(1982, 1, 1)));
        people.add(new PersonDto("4", "Rachel", new Date(1983, 1, 1)));
        people.add(new PersonDto("5", "Sarah", new Date(1984, 1, 1)));
        people.add(new PersonDto("6", "Brad", new Date(1985, 1, 1)));
        people.add(new PersonDto("7", "Phillip", new Date(1986, 1, 1)));
        people.add(new PersonDto("8", "Cory", new Date(1987, 1, 1)));
        people.add(new PersonDto("9", "Burt", new Date(1988, 1, 1)));
        people.add(new PersonDto("10", "Gladis", new Date(1989, 1, 1)));
        people.add(new PersonDto("11", "Ethel", new Date(1990, 1, 1)));
        people.add(new PersonDto("12", "Terry", new Date(1991, 1, 1)));
        people.add(new PersonDto("13", "Bernie", new Date(1992, 1, 1)));
        people.add(new PersonDto("14", "Will", new Date(1993, 1, 1)));
        people.add(new PersonDto("15", "Jim", new Date(1994, 1, 1)));
        people.add(new PersonDto("16", "Eva", new Date(1995, 1, 1)));
        people.add(new PersonDto("17", "Susan", new Date(1996, 1, 1)));
        people.add(new PersonDto("18", "Justin", new Date(1997, 1, 1)));
        people.add(new PersonDto("19", "Gerry", new Date(1998, 1, 1)));
        people.add(new PersonDto("20", "Fitz", new Date(1999, 1, 1)));
        people.add(new PersonDto("21", "Ellie", new Date(2000, 1, 1)));
        people.add(new PersonDto("22", "Gordon", new Date(2001, 1, 1)));
        people.add(new PersonDto("23", "Gail", new Date(2002, 1, 1)));
        people.add(new PersonDto("24", "Gary", new Date(2003, 1, 1)));
        people.add(new PersonDto("25", "Gabby", new Date(2004, 1, 1)));
        people.add(new PersonDto("26", "Jeanie", new Date(2005, 1, 1)));

        return people.stream().skip(skip).limit(count).collect(Collectors.toList());
    }

    private List<PersonDto> getLargePersonList(int count, int skip)
    {
        List<PersonDto> people = getPersonList(count, skip);
        people.stream().forEach(x -> x.setName(getLongName(x.getName())));

        return people;
    }

    public String getLongName(String name)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            sb.append(" " + name);

        return sb.toString();
    }
}
