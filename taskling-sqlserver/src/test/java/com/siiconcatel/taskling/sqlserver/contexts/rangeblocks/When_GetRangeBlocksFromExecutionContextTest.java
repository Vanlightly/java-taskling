package com.siiconcatel.taskling.sqlserver.contexts.rangeblocks;

import com.siiconcatel.taskling.core.blocks.rangeblocks.*;
import com.sun.jndi.dns.DnsUrl;
import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.NumericRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.core.events.EventType;
import com.siiconcatel.taskling.core.utils.TicksHelper;
import com.siiconcatel.taskling.sqlserver.categories.BlocksTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.helpers.*;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class When_GetRangeBlocksFromExecutionContextTest {
    private ExecutionsHelper executionHelper;
    private BlocksHelper blocksHelper;
    private int taskDefinitionId;
    private int MaxBlocks = 2000;

    public When_GetRangeBlocksFromExecutionContextTest()
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
    public void If_AsDateRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        // ARRANGE
        int blockCountLimit = 10;

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(blockCountLimit);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = Instant.now().minus(Duration.ofHours(12));
            Instant toDate = Instant.now();
            Duration maxBlockRange = Duration.ofMinutes(30);

            List<DateRangeBlockContext> rangeBlocks = executionContext.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)).getBlockContexts();
            assertEquals(10, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
            int expectedNotStartedCount = blockCountLimit;
            int expectedCompletedCount = 0;
            assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
            assertEquals(0, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
            assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

            for (DateRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                expectedNotStartedCount--;
                assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
                assertEquals(1, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));

                // processing here
                rangeBlock.complete();
                expectedCompletedCount++;
                assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));
            }
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = Instant.now();
            Instant toDate = Instant.now().minus(Duration.ofHours(12));
            Duration maxBlockRange = Duration.ofMinutes(30);
            List<DateRangeBlockContext> rangeBlocks = executionContext.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)).getBlockContexts();
            assertEquals(0, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

            LastEvent lastEvent = executionHelper.getLastEvent(taskDefinitionId);
            assertEquals(EventType.CheckPoint, lastEvent.Type);
            assertEquals("No values for generate the block. Emtpy Block context returned.", lastEvent.Description);
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRangeNoBlockNeeded_ThenEmptyListAndEventPersisted()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            long fromNumber = 1000;
            long toNumber = 800;
            long maxBlockRange = 100;
            List<NumericRangeBlockContext> rangeBlocks = executionContext.getNumericRangeBlocks(x -> x.withRange(fromNumber, toNumber, maxBlockRange)).getBlockContexts();
            assertEquals(0, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));

            LastEvent lastEvent = executionHelper.getLastEvent(taskDefinitionId);
            assertEquals(EventType.CheckPoint, lastEvent.Type);
            assertEquals("No values for generate the block. Emtpy Block context returned.", lastEvent.Description);
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRange_NumberOfBlocksAndStatusesOfBlockExecutionsCorrectAtEveryStep()
    {
        // ARRANGE
        int blockCountLimit = 10;

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(blockCountLimit);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            long fromNumber = 1000;
            long toNumber = 3000;
            long maxBlockRange = 100;
            List<NumericRangeBlockContext> blocks = executionContext.getNumericRangeBlocks(x -> x.withRange(fromNumber, toNumber, maxBlockRange)).getBlockContexts();
            assertEquals(10, blocksHelper.getBlockCount(TestConstants.ApplicationName, TestConstants.TaskName));
            int expectedNotStartedCount = blockCountLimit;
            int expectedCompletedCount = 0;
            assertEquals(expectedNotStartedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.NotStarted));
            assertEquals(0, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Started));
            assertEquals(expectedCompletedCount, blocksHelper.getBlockExecutionCountByStatus(TestConstants.ApplicationName, TestConstants.TaskName, BlockExecutionStatus.Completed));

            for (NumericRangeBlockContext block : blocks)
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
    public void If_AsNumericRange_BlocksDoNotShareIds()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            long fromNumber = 0;
            long toNumber = 100;
            long maxBlockRange = 10;
            List<NumericRangeBlockContext> blocks = executionContext.getNumericRangeBlocks(x -> x.withRange(fromNumber, toNumber, maxBlockRange)).getBlockContexts();

            int counter = 0;
            NumericRangeBlockContext lastBlock = null;
            for (NumericRangeBlockContext block : blocks)
            {
                if (counter > 0)
                    assertEquals(lastBlock.getNumericRangeBlock().getEndNumber() + 1, block.getNumericRangeBlock().getStartNumber());

                lastBlock = block;
                counter++;
            }
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRange_PreviousBlock_ThenLastBlockContainsDates()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = TimeHelper.getInstant(2016, 1, 1);
            Instant toDate = TimeHelper.getInstant(2016, 1, 31, 23, 59, 59);
            Duration maxBlockRange = Duration.ofDays(1);

            List<DateRangeBlockContext> rangeBlocks = prev.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)
                .overrideConfiguration()
                .maximumBlocksToGenerate(50)).getBlockContexts();

            for (DateRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        prev.complete();

        long fromTicks = TicksHelper.getTicksFromDate(TimeHelper.getInstant(2016, 1, 31));
        long toTicks = TicksHelper.getTicksFromDate(TimeHelper.getInstant(2016, 1, 31, 23, 59, 59));
        DateRangeBlock expectedLastBlock = new RangeBlock("0", 1, fromTicks, toTicks, BlockType.DateRange);

        // ACT
        DateRangeBlock lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastDateRangeBlock(LastBlockOrder.LastCreated);
        }
        executionContext.complete();

        // ASSERT
        assertEquals(expectedLastBlock.getStartDate(), lastBlock.getStartDate());
        assertEquals(expectedLastBlock.getEndDate(), lastBlock.getEndDate());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRange_NoPreviousBlock_ThenLastBlockIsNull()
    {
        // ARRANGE
        // all previous blocks were deleted in TestInitialize

        // ACT
        DateRangeBlock lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastDateRangeBlock(LastBlockOrder.LastCreated);
        }
        executionContext.complete();

        // ASSERT
        assertNull(lastBlock);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = TimeHelper.getInstant(2016, 1, 1);
            Instant toDate = TimeHelper.getInstant(2016, 1, 2);
            Duration maxBlockRange = Duration.ofDays(2);

            List<DateRangeBlockContext> rangeBlocks = prev.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)
                .overrideConfiguration()
                .maximumBlocksToGenerate(50)).getBlockContexts();

            for (DateRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        prev.complete();

        blocksHelper.insertPhantomDateRangeBlock(TestConstants.ApplicationName,
                TestConstants.TaskName,
                TimeHelper.getInstant(2015, 1, 1),
                TimeHelper.getInstant(2015, 1, 2));

        // ACT
        DateRangeBlock lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastDateRangeBlock(LastBlockOrder.LastCreated);
        }
        executionContext.complete();

        // ASSERT
        assertEquals(TimeHelper.getInstant(2016, 1, 1), lastBlock.getStartDate());
        assertEquals(TimeHelper.getInstant(2016, 1, 2), lastBlock.getEndDate());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRange_PreviousBlock_ThenLastBlockContainsDates()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<NumericRangeBlockContext> rangeBlocks = prev.getNumericRangeBlocks(x -> x.withRange(1, 1000, 100)).getBlockContexts();

            for (NumericRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        prev.complete();

        NumericRangeBlock expectedLastBlock = new RangeBlock("0", 1, 901, 1000, BlockType.NumericRange);

        // ACT
        NumericRangeBlock lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastNumericRangeBlock(LastBlockOrder.RangeStart);
        }
        executionContext.complete();

        // ASSERT
        assertEquals(expectedLastBlock.getStartNumber(), lastBlock.getStartNumber());
        assertEquals(expectedLastBlock.getEndNumber(), lastBlock.getEndNumber());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRange_NoPreviousBlock_ThenLastBlockIsNull()
    {
        // ARRANGE
        // all previous blocks were deleted in TestInitialize

        // ACT
        NumericRangeBlock lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastNumericRangeBlock(LastBlockOrder.LastCreated);
        }
        executionContext.complete();

        // ASSERT
        assertNull(lastBlock);
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRange_PreviousBlockIsPhantom_ThenLastBlockIsNotThePhantom()
    {
        // ARRANGE
        // Create previous blocks
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<NumericRangeBlockContext> rangeBlocks = prev.getNumericRangeBlocks(x -> x.withRange(1000, 2000, 2000)).getBlockContexts();

            for (NumericRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        prev.complete();

        blocksHelper.insertPhantomNumericBlock(TestConstants.ApplicationName, TestConstants.TaskName, 0, 100);

        // ACT
        NumericRangeBlock lastBlock = null;
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            lastBlock = executionContext.getLastNumericRangeBlock(LastBlockOrder.LastCreated);
        }
        executionContext.complete();

        // ASSERT
        assertEquals(1000, (int)lastBlock.getStartNumber());
        assertEquals(2000, (int)lastBlock.getEndNumber());
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
    {
        // ARRANGE
        String referenceValue = UUID.randomUUID().toString();

        // ACT and // ASSERT
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = Instant.now().minus(Duration.ofHours(12));
            Instant toDate = Instant.now();
            Duration maxBlockRange = Duration.ofMinutes(30);
            List<DateRangeBlockContext> rangeBlocks = prev.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)
                .overrideConfiguration()
                .maximumBlocksToGenerate(5)).getBlockContexts();

            rangeBlocks.get(0).start();
            rangeBlocks.get(0).complete(); // completed
            rangeBlocks.get(1).start();
            rangeBlocks.get(1).failed("Something bad happened"); // failed
            // 2 not started
            rangeBlocks.get(3).start(); // started
            rangeBlocks.get(4).start();
            rangeBlocks.get(4).complete(); // completed
        }
        prev.complete();

        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<DateRangeBlockContext> rangeBlocks = executionContext.getDateRangeBlocks(x -> x.reprocessDateRange()
                .pendingAndFailedBlocks()
                .ofExecutionWith(referenceValue)).getBlockContexts();

            assertEquals(3, rangeBlocks.size());
        }
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
    {
        // ARRANGE
        String referenceValue = UUID.randomUUID().toString();

        // ACT and // ASSERT
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = Instant.now().minus(Duration.ofHours(12));
            Instant toDate = Instant.now();
            Duration maxBlockRange = Duration.ofMinutes(30);
            List<DateRangeBlockContext> rangeBlocks = prev.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)
                .overrideConfiguration()
                .maximumBlocksToGenerate(5)).getBlockContexts();

            rangeBlocks.get(0).start();
            rangeBlocks.get(0).complete(); // completed
            rangeBlocks.get(1).start();
            rangeBlocks.get(1).failed(); // failed
            // 2 not started
            rangeBlocks.get(3).start(); // started
            rangeBlocks.get(4).start();
            rangeBlocks.get(4).complete(); // completed
        }
        prev.complete();

        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<DateRangeBlockContext> rangeBlocks = executionContext.getDateRangeBlocks(x -> x.reprocessDateRange()
                .allBlocks()
                .ofExecutionWith(referenceValue)).getBlockContexts();

            assertEquals(5, rangeBlocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackTheFailedBlockWhenRequested()
    {
        // ARRANGE
        String referenceValue = UUID.randomUUID().toString();

        // ACT and // ASSERT
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            long fromNumber = 1000;
            long toNumber = 3000;
            long maxBlockRange = 100;
            List<NumericRangeBlockContext> blocks = prev.getNumericRangeBlocks(x -> x.withRange(fromNumber, toNumber, maxBlockRange)
                .overrideConfiguration()
                .maximumBlocksToGenerate(5)).getBlockContexts();

            blocks.get(0).start();
            blocks.get(0).complete(); // completed
            blocks.get(1).start();
            blocks.get(1).failed(); // failed
            // 2 not started
            blocks.get(3).start(); // started
            blocks.get(4).start();
            blocks.get(4).complete(); // completed
        }
        prev.complete();

        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<NumericRangeBlockContext> rangeBlocks = executionContext.getNumericRangeBlocks(x -> x.reprocessNumericRange()
                .pendingAndFailedBlocks()
                .ofExecutionWith(referenceValue)).getBlockContexts();

            assertEquals(3, rangeBlocks.size());
        }
        executionContext.complete();
    }


    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRange_PreviousExecutionHadOneFailedBlockAndMultipleOkOnes_ThenBringBackAllBlocksWhenRequested()
    {
        // ARRANGE
        String referenceValue = UUID.randomUUID().toString();

        // ACT and // ASSERT
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart(referenceValue);
        assertTrue(startedOk);
        if (startedOk)
        {
            long fromNumber = 1000;
            long toNumber = 3000;
            long maxBlockRange = 100;
            List<NumericRangeBlockContext> blocks = prev.getNumericRangeBlocks(x -> x.withRange(fromNumber, toNumber, maxBlockRange)
                .overrideConfiguration()
                .maximumBlocksToGenerate(5)).getBlockContexts();

            blocks.get(0).start();
            blocks.get(0).complete(); // completed
            blocks.get(1).start();
            blocks.get(1).failed(); // failed
            // 2 not started
            blocks.get(3).start(); // started
            blocks.get(4).start();
            blocks.get(4).complete(); // completed
        }
        prev.complete();

        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<NumericRangeBlockContext> rangeBlocks = executionContext.getNumericRangeBlocks(x -> x.reprocessNumericRange()
                .allBlocks()
                .ofExecutionWith(referenceValue)).getBlockContexts();

            assertEquals(5, rangeBlocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRangeWithPreviousDeadBlocks_ThenReprocessOk()
    {
        // ARRANGE
        createFailedDateTask();
        createDeadDateTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = TimeHelper.getInstant(2016, 1, 7);
            Instant toDate = TimeHelper.getInstant(2016, 1, 7);
            Duration maxBlockRange = Duration.ofDays(1);

            List<DateRangeBlockContext> dateBlocks = executionContext.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)
                .overrideConfiguration()
                .reprocessDeadTasks(Duration.ofDays(1), (short)3)
                .reprocessFailedTasks(Duration.ofDays(1), (short)3)
                .maximumBlocksToGenerate(8)).getBlockContexts();

            int counter = 0;
            for (DateRangeBlockContext block : dateBlocks)
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
        createFailedDateTask();
        createDeadDateTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = TimeHelper.getInstant(2016, 1, 7);
            Instant toDate = TimeHelper.getInstant(2016, 1, 31);
            Duration maxBlockRange = Duration.ofDays(1);

            List<DateRangeBlockContext> dateBlocks = executionContext.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)
                .overrideConfiguration()
                .reprocessDeadTasks(Duration.ofDays(1), (short)3)
                .reprocessFailedTasks(Duration.ofDays(1), (short)3)
                .maximumBlocksToGenerate(8)).getBlockContexts();

            assertEquals(8, dateBlocks.size());
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 1))));
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 2))));
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 3))));
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 4))));
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 5))));
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 6))));
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 7))));
            assertTrue(dateBlocks.stream().anyMatch(x -> x.getDateRangeBlock().getStartDate().equals(TimeHelper.getInstant(2016, 1, 8))));
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        // ARRANGE
        int blockCountLimit = 10;
        createFailedDateTask();
        createDeadDateTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(blockCountLimit);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant fromDate = TimeHelper.getInstant(2016, 1, 7);
            Instant toDate = TimeHelper.getInstant(2016, 1, 31);
            Duration maxBlockRange = Duration.ofDays(1);

            List<DateRangeBlockContext> rangeBlocks = executionContext.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)).getBlockContexts();
            assertEquals(10, rangeBlocks.size());

            assertTrue(rangeBlocks.stream().allMatch(x -> x.getDateRangeBlock().getStartDate().isAfter(fromDate)
                            || x.getDateRangeBlock().getStartDate().equals(fromDate)));
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRangeWithOverridenConfiguration_ThenOverridenValuesAreUsed()
    {
        // ARRANGE
        createFailedNumericTask();
        createDeadNumericTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            long from = 61;
            long to = 200;
            long maxBlockSize = 10;
            List<NumericRangeBlockContext> numericBlocks = executionContext.getNumericRangeBlocks(x -> x.withRange(from, to, maxBlockSize)
                .overrideConfiguration()
                .reprocessDeadTasks(Duration.ofDays(1), (short)3)
                .reprocessFailedTasks(Duration.ofDays(1), (short)3)
                .maximumBlocksToGenerate(8)).getBlockContexts();

            assertEquals(8, numericBlocks.size());
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 1));
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 11));
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 21));
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 31));
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 41));
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 51));
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 61));
            assertTrue(numericBlocks.stream().anyMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() == 71));
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRangeWithNoOverridenConfiguration_ThenConfigurationValuesAreUsed()
    {
        // ARRANGE
        int blockCountLimit = 10;
        createFailedNumericTask();
        createDeadNumericTask();

        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(blockCountLimit);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            long from = 61;
            long to = 200;
            long maxBlockSize = 10;
            List<NumericRangeBlockContext> numericBlocks = executionContext.getNumericRangeBlocks(x -> x.withRange(from, to, maxBlockSize)).getBlockContexts();
            assertEquals(10, numericBlocks.size());
            assertTrue(numericBlocks.stream().allMatch(x -> (int)x.getNumericRangeBlock().getStartNumber() >= 61));
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsDateRange_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
    {
        // ARRANGE
        Instant fromDate = Instant.now().minus(Duration.ofHours(12));
        Instant toDate = Instant.now();

        // create a block
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Duration maxBlockRange = Duration.ofDays(1);
            List<DateRangeBlockContext> rangeBlocks = prev.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange)).getBlockContexts();
            for (DateRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        prev.complete();

        // add this processed block to the forced queue
        long lastBlockId = blocksHelper.getLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
        blocksHelper.enqueueForcedBlock(lastBlockId);

        // ACT - reprocess the forced block
        TaskExecutionContext reprocContext = createTaskExecutionContext(MaxBlocks);
        startedOk = reprocContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<DateRangeBlockContext> rangeBlocks = reprocContext.getDateRangeBlocks(x -> x.onlyOldDateBlocks()).getBlockContexts();
            assertEquals(1, rangeBlocks.size());
            assertTrue(Math.abs(Duration.between(fromDate, rangeBlocks.get(0).getDateRangeBlock().getStartDate()).getSeconds()) <= 1);
            assertTrue(Math.abs(Duration.between(toDate, rangeBlocks.get(0).getDateRangeBlock().getEndDate()).getSeconds()) <= 1);

            for (DateRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        reprocContext.complete();

        // The forced block will have been dequeued so it should not be processed again
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<DateRangeBlockContext> rangeBlocks = executionContext.getDateRangeBlocks(x -> x.onlyOldDateBlocks()).getBlockContexts();
            assertEquals(0, rangeBlocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_AsNumericRange_ForceBlock_ThenBlockGetsReprocessedAndDequeued()
    {
        // ARRANGE
        long fromNumber = 1000;
        long toNumber = 2000;

        // create a block
        TaskExecutionContext prev = createTaskExecutionContext(MaxBlocks);
        boolean startedOk = prev.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            long maxBlockRange = 2000;
            List<NumericRangeBlockContext> rangeBlocks = prev.getNumericRangeBlocks(x -> x.withRange(fromNumber, toNumber, maxBlockRange)).getBlockContexts();
            for (NumericRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        prev.complete();

        // add this processed block to the forced queue
        long lastBlockId = blocksHelper.getLastBlockId(TestConstants.ApplicationName, TestConstants.TaskName);
        blocksHelper.enqueueForcedBlock(lastBlockId);

        // ACT - reprocess the forced block
        TaskExecutionContext reprocContext = createTaskExecutionContext(MaxBlocks);
        startedOk = reprocContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<NumericRangeBlockContext> rangeBlocks = reprocContext.getNumericRangeBlocks(x -> x.onlyOldNumericBlocks()).getBlockContexts();
            assertEquals(1, rangeBlocks.size());
            assertEquals(fromNumber, rangeBlocks.get(0).getNumericRangeBlock().getStartNumber());
            assertEquals(toNumber, rangeBlocks.get(0).getNumericRangeBlock().getEndNumber());

            for (NumericRangeBlockContext rangeBlock : rangeBlocks)
            {
                rangeBlock.start();
                rangeBlock.complete();
            }
        }
        reprocContext.complete();

        // The forced block will have been dequeued so it should not be processed again
        TaskExecutionContext executionContext = createTaskExecutionContext(MaxBlocks);
        startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            List<NumericRangeBlockContext> rangeBlocks = executionContext.getNumericRangeBlocks(x -> x.onlyOldNumericBlocks()).getBlockContexts();
            assertEquals(0, rangeBlocks.size());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_DateRangeLargerThanMaxBlocksAllows_ThenResponseIndicatesTotalDateRangeNotCovered()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(4);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            Instant now = Instant.now();
            Instant fromDate = now.minus(Duration.ofHours(7));
            Instant toDate = now;
            Duration maxBlockRange = Duration.ofMinutes(60);
            DateRangeBlockResponse rangeBlockResponse = executionContext.getDateRangeBlocks(x -> x.withRange(fromDate, toDate, maxBlockRange));
            assertFalse(rangeBlockResponse.isRangeCovered());
            assertEquals(now.minus(Duration.ofHours(3)).toEpochMilli(), rangeBlockResponse.getIncludedRangeEnd().toEpochMilli());
            assertEquals(now.toEpochMilli(), rangeBlockResponse.getExcludedRangeEnd().toEpochMilli());
        }
        executionContext.complete();
    }

    @Category({FastTests.class, BlocksTests.class})
    @Test
    public void If_NumericRangeLargerThanMaxBlocksAllows_ThenResponseIndicatesTotalNumericRangeNotCovered()
    {
        // ARRANGE
        // ACT and // ASSERT
        TaskExecutionContext executionContext = createTaskExecutionContext(4);
        boolean startedOk = executionContext.tryStart();
        assertTrue(startedOk);
        if (startedOk)
        {
            long from = 1;
            long to = 70;
            long maxBlockRange = 10;
            NumericRangeBlockResponse rangeBlockResponse = executionContext.getNumericRangeBlocks(x -> x.withRange(from, to, maxBlockRange));
            assertFalse(rangeBlockResponse.isRangeCovered());
            assertEquals(40, rangeBlockResponse.getIncludedRangeEnd());
            assertEquals(70, rangeBlockResponse.getExcludedRangeEnd());
        }
        executionContext.complete();
    }

    private TaskExecutionContext createTaskExecutionContext(int maxBlocksToCreate)
    {
        return ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndReprocessing(maxBlocksToCreate));
    }

    private TaskExecutionContext createTaskExecutionContextWithNoReprocessing(int maxBlocksToCreate)
    {
        return ClientHelper.getExecutionContext(TestConstants.TaskName, ClientHelper.getDefaultTaskConfigurationWithKeepAliveAndNoReprocessing(maxBlocksToCreate));
    }

    private void createFailedDateTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            Instant from = TimeHelper.getInstant(2016, 1, 1);
            Instant to = TimeHelper.getInstant(2016, 1, 4);
            Duration maxBlockSize = Duration.ofDays(1);
            List<DateRangeBlockContext> dateBlocks = executionContext.getDateRangeBlocks(x -> x.withRange(from, to, maxBlockSize)).getBlockContexts();

            for (DateRangeBlockContext block : dateBlocks)
            {
                block.start();
                block.failed();
            }
        }
        executionContext.complete();
    }

    private void createDeadDateTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            Instant from = TimeHelper.getInstant(2016, 1, 4);
            Instant to = TimeHelper.getInstant(2016, 1, 7);
            Duration maxBlockSize = Duration.ofDays(1);
            List<DateRangeBlockContext> dateBlocks = executionContext.getDateRangeBlocks(x -> x.withRange(from, to, maxBlockSize)).getBlockContexts();

            for (DateRangeBlockContext block : dateBlocks)
            {
                block.start();
            }
        }
        executionContext.complete();

        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.setLastExecutionAsDead(taskDefinitionId);
    }

    private void createFailedNumericTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            long from = 1;
            long to = 30;
            short maxBlockSize = 10;
            List<NumericRangeBlockContext> numericBlocks = executionContext.getNumericRangeBlocks(x -> x.withRange(from, to, maxBlockSize)).getBlockContexts();

            for (NumericRangeBlockContext block : numericBlocks)
            {
                block.start();
                block.failed();
            }
        }
        executionContext.complete();
    }

    private void createDeadNumericTask()
    {
        TaskExecutionContext executionContext = createTaskExecutionContextWithNoReprocessing(MaxBlocks);
        boolean startedOk = executionContext.tryStart();
        if (startedOk)
        {
            long from = 31;
            long to = 60;
            short maxBlockSize = 10;
            List<NumericRangeBlockContext> numericBlocks = executionContext.getNumericRangeBlocks(x -> x.withRange(from, to, maxBlockSize)).getBlockContexts();

            for (NumericRangeBlockContext block : numericBlocks)
            {
                block.start();
            }
        }
        executionContext.complete();

        ExecutionsHelper executionHelper = new ExecutionsHelper();
        executionHelper.setLastExecutionAsDead(taskDefinitionId);
    }
}
