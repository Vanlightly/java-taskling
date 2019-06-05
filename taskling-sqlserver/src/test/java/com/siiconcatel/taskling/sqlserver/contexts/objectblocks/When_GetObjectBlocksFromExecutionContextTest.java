package com.siiconcatel.taskling.sqlserver.contexts.objectblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlockImpl;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlockResponse;
import com.siiconcatel.taskling.core.contexts.ObjectBlockContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.core.utils.StringUtils;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.helpers.*;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static junit.framework.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.*;

public class When_GetObjectBlocksFromExecutionContextTest {
    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;
    private int taskDefinitionId = 0;
    private int MaxBlocks = 10000;

    public When_GetObjectBlocksFromExecutionContextTest()
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
    public void If_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);

        if (startedOk)
        {

            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.withObject("Testing1")).getBlockContexts();
            assertEquals(1, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
            int expectedNotStartedCount = 1;
            int expectedCompletedCount = 0;
            assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
            assertEquals(0, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
            assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();
                expectedNotStartedCount--;
                assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                assertEquals(1, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));



                // processing here
                block.complete();
                expectedCompletedCount++;
                assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
            }
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_NoBlockNeeded_ThenEmptyListAndEventPersisted()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.withNoNewBlocks()).getBlockContexts();
            assertEquals(0, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

            LastEvent lastEvent = executionHelper.getLastEvent(taskDefinitionId);
            assertEquals(EventType.CheckPoint, lastEvent.Type);
            assertEquals("No values for generate the block. Emtpy Block context returned.", lastEvent.Description);
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_ComplexObjectStored_ThenRetrievedOk()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            MyComplexClass myObject = new MyComplexClass(
                10,
                "Rupert",
                new Date(1955, 1, 1),
                new MyOtherComplexClass(12.6d,
                    new ArrayList<String>(Arrays.asList("hello", "goodbye", null))));


            ObjectBlockContext<MyComplexClass> block = executionContext.getObjectBlocks(
                    MyComplexClass.class,
                    x -> x.withObject(myObject)).getBlockContexts().get(0);

            assertEquals(myObject.getId(), block.getBlock().getObject().getId());
            assertEquals(myObject.getName(), block.getBlock().getObject().getName());
            assertEquals(myObject.getDateOfBirth(), block.getBlock().getObject().getDateOfBirth());
            assertEquals(myObject.getSomeOtherData().getValue(), block.getBlock().getObject().getSomeOtherData().getValue());
            assertEquals(myObject.getSomeOtherData().getNotes().get(0), block.getBlock().getObject().getSomeOtherData().getNotes().get(0));
            assertEquals(myObject.getSomeOtherData().getNotes().get(1), block.getBlock().getObject().getSomeOtherData().getNotes().get(1));
            assertNull(block.getBlock().getObject().getSomeOtherData().getNotes().get(2));
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_LargeComplexObjectStored_ThenRetrievedOk()
    {
        List<String> longList = getLargeListOfStrings();

        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            MyComplexClass myObject = new MyComplexClass(
                    10,
                    "Rupert",
                    new Date(1955, 1, 1),
                    new MyOtherComplexClass(12.6d, longList));

            ObjectBlockContext<MyComplexClass> block = executionContext.getObjectBlocks(
                    MyComplexClass.class,
                    x -> x.withObject(myObject)).getBlockContexts().get(0);

            assertEquals(myObject.getId(), block.getBlock().getObject().getId());
            assertEquals(myObject.getName(), block.getBlock().getObject().getName());
            assertEquals(myObject.getDateOfBirth(), block.getBlock().getObject().getDateOfBirth());
            assertEquals(myObject.getSomeOtherData().getValue(),block.getBlock().getObject().getSomeOtherData().getValue());
            assertEquals(myObject.getSomeOtherData().getNotes().size(), block.getBlock().getObject().getSomeOtherData().getNotes().size());

            for (int i = 0; i < myObject.getSomeOtherData().getNotes().size(); i++)
            {
                assertEquals(myObject.getSomeOtherData().getNotes().get(i), block.getBlock().getObject().getSomeOtherData().getNotes().get(i));
            }
        }
        executionContext.complete();
    }

    private List<String> getLargeListOfStrings()
    {
        List<String> list = new ArrayList<>();

        for (int i = 0; i < 1000; i++)
            list.add("Long value is " + UUID.randomUUID().toString());

        return list;
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_PreviousBlock_ThenLastBlockHasCorrectObjectValue()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext();
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = prev.getObjectBlocks(
                    String.class,
                    x -> x.withObject("Testing123")).getBlockContexts();

            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();
                block.complete();
            }
        }
        prev.complete();

        ObjectBlock<String> expectedLastBlock = new ObjectBlockImpl<String>("1", 1, "Testing123");


        // ACT
        ObjectBlock<String> lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext();
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastObjectBlock(String.class);
        }
        executionContext.complete();

        // ASSERT
        assertEquals(expectedLastBlock.getObject(), lastBlock.getObject());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_NoPreviousBlock_ThenLastBlockIsNull()
    {
        // ARRANGE
        // all previous blocks were deleted in TestInitialize

        // ACT
        ObjectBlock<String> lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastObjectBlock(String.class);
        }
        executionContext.complete();

        // ASSERT
        assertNull(lastBlock);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext();
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = prev.getObjectBlocks(
                    String.class,
                    x -> x.withObject("Testing987")).getBlockContexts();

            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();
                block.complete();
            }
        }
        prev.complete();

        blocksHelper.insertPhantomObjectBlock(TestConstants.ApplicationName, TestConstants.TaskName);

        // ACT
        ObjectBlock<String> lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext();
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastObjectBlock(String.class);
        }
        executionContext.complete();

        // ASSERT
        assertEquals("Testing987", lastBlock.getObject());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
    {
        // ARRANGE
        String referenceValue = UUID.randomUUID().toString();

        // ACT and // ASSERT

        TaskExecutionContext prev = createTaskExecutionContext();
        boolean startedOk = prev.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = new ArrayList<>();
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object1")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object2")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object3")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object4")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object5")).getBlockContexts());

            blocks.get(0).start();
            blocks.get(0).complete(); // completed
            blocks.get(1).start();
            blocks.get(1).failed("Something bad happened"); // failed
            // 2 not started
            blocks.get(3).start(); // started
            blocks.get(4).start();
            blocks.get(4).complete(); // completed
        }
        prev.complete();

        TaskExecutionContext executionContext = createTaskExecutionContext();
        startedOk = executionContext.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.reprocess()
                        .pendingAndFailedBlocks()
                        .ofExecutionWith(referenceValue)).getBlockContexts();

            assertEquals(3, blocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
    {
        // ARRANGE
        String referenceValue = UUID.randomUUID().toString();

        // ACT and // ASSERT
        TaskExecutionContext prev = createTaskExecutionContext();
        boolean startedOk = prev.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = new ArrayList<>();
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object1")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object2")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object3")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object4")).getBlockContexts());
            blocks.addAll(prev.getObjectBlocks(String.class, x -> x.withObject("My object5")).getBlockContexts());

            blocks.get(0).start();
            blocks.get(0).complete(); // completed
            blocks.get(1).start();
            blocks.get(1).failed("Something bad happened"); // failed
            // 2 not started
            blocks.get(3).start(); // started
            blocks.get(4).start();
            blocks.get(4).complete(); // completed
        }

        prev.complete();

        TaskExecutionContext executionContext = createTaskExecutionContext();
        startedOk = executionContext.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.reprocess()
                            .allBlocks()
                            .ofExecutionWith(referenceValue)).getBlockContexts();

            assertEquals(5, blocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_WithPreviousDeadBlocks_ThenReprocessOk()
    {
        // ARRANGE
        createFailedObjectBlockTask();
        createDeadObjectBlockTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.withObject("TestingDFG")
                            .overrideConfiguration()
                            .reprocessDeadTasks(Duration.ofDays(1), (short)3)
                            .reprocessFailedTasks(Duration.ofDays(1), (short)3)
                            .maximumBlocksToGenerate(8)).getBlockContexts();

            int counter = 0;
            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();

                block.complete();

                counter++;
                assertEquals(counter, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
            }


        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
    {
        // ARRANGE
        createFailedObjectBlockTask();
        createDeadObjectBlockTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.withObject("TestingYHN")
                        .overrideConfiguration()
                        .reprocessDeadTasks(Duration.ofDays(1), (short)3)
                        .reprocessFailedTasks(Duration.ofDays(1), (short)3)
                        .maximumBlocksToGenerate(8)).getBlockContexts();

            assertEquals(3, blocks.size());
            assertTrue(blocks.stream().anyMatch(x -> x.getBlock().getObject().equals("Dead Task")));
            assertTrue(blocks.stream().anyMatch(x -> x.getBlock().getObject().equals("Failed Task")));
            assertTrue(blocks.stream().anyMatch(x -> x.getBlock().getObject().equals("TestingYHN")));
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_WithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        // ARRANGE
        createFailedObjectBlockTask();
        createDeadObjectBlockTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.withObject("Testing YUI")).getBlockContexts();
            assertEquals(1, blocks.size());
            assertTrue(blocks.get(0).getBlock().getObject().equals("Testing YUI"));
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
    {
        // ARRANGE
        Date fromDate = Date.from(Instant.now().minus(Duration.ofHours(12)));
        Date toDate = Date.from(Instant.now());

        // create a block
        TaskExecutionContext prev = createTaskExecutionContext();
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = prev.getObjectBlocks(
                    String.class,
                    x -> x.withObject("Testing Hello")).getBlockContexts();

            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();
                block.complete();
            }
        }
        prev.complete();

        // add this processed block to the forced queue
        long lastBlockId = blocksHelper.getLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
        blocksHelper.enqueueForcedBlock(lastBlockId);

        // ACT - reprocess the forced block
        TaskExecutionContext reprocContext = createTaskExecutionContext();
        startedOk = reprocContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = reprocContext.getObjectBlocks(
                    String.class,
                    x -> x.withObject("Testing Goodbye")).getBlockContexts();
            assertEquals(2, blocks.size());
            assertEquals("Testing Hello", blocks.get(0).getBlock().getObject());
            assertEquals("Testing Goodbye", blocks.get(1).getBlock().getObject());
            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();
                block.complete();
            }
        }
        reprocContext.complete();

        // The forced block will have been dequeued so it should not be processed again
        TaskExecutionContext executionContext = createTaskExecutionContext();
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.withNoNewBlocks()).getBlockContexts();
            assertEquals(0, blocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_IfNewBlockCannotBeIncludedDueToMaxBlockLimit_ThenResponseIndicatesNewObjectNotIncluded()
    {
        // ARRANGE
        createFailedObjectBlockTask();
        createDeadObjectBlockTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(2);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            ObjectBlockResponse<String> blockResponse = executionContext.getObjectBlocks(
                    String.class,
                    x -> x.withObject("new-block"));
            assertFalse(blockResponse.isObjectIncluded());
            assertEquals(2, blockResponse.getBlockContexts().size());
        }
        executionContext.complete();
    }

    private TaskExecutionContext createTaskExecutionContext(int maxBlocks)
    {
        return ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocks));
    }

    private TaskExecutionContext createTaskExecutionContext()
    {
        return ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(MaxBlocks));
    }

    private TaskExecutionContext createTaskExecutionContextWithNoReprocessing()
    {
        return ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(MaxBlocks));
    }

    private void createFailedObjectBlockTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(String.class,
                    x -> x.withObject("Failed Task")).getBlockContexts();

            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();
                block.failed();
            }
        }
        executionContext.complete();
    }

    private void createDeadObjectBlockTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing();
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            List<ObjectBlockContext<String>> blocks = executionContext.getObjectBlocks(String.class,
                    x -> x.withObject("Dead Task")).getBlockContexts();

            for (ObjectBlockContext<String> block : blocks)
            {
                block.start();
            }
        }
        executionContext.complete();

        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.setLastExecutionAsDead(taskDefinitionId);
    }
}
