package com.siiconcatel.taskling.core.blocks.factories;

import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlockResponse;
import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlockResponse;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.RangeBlockContextImpl;
import com.siiconcatel.taskling.core.blocks.requests.BlockRequest;
import com.siiconcatel.taskling.core.blocks.requests.DateRangeBlockRequest;
import com.siiconcatel.taskling.core.blocks.requests.NumericRangeBlockRequest;
import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.NumericRangeBlockContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.RangeBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindBlocksOfTaskRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindDeadBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindFailedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.ForcedRangeBlockQueueItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.QueuedForcedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.rangeblocks.RangeBlockCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.utils.StringUtils;
import com.siiconcatel.taskling.core.utils.WaitUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class RangeBlockFactoryImpl extends BlockFactoryBase implements RangeBlockFactory {
    private final RangeBlockRepository rangeBlockRepository;

    public RangeBlockFactoryImpl(BlockRepository blockRepository,
                                  TaskExecutionRepository taskExecutionRepository,
                                  RangeBlockRepository rangeBlockRepository)
    {
        super(taskExecutionRepository, blockRepository);
        this.rangeBlockRepository = rangeBlockRepository;
    }

    public DateRangeBlockResponse generateDateRangeBlocks(DateRangeBlockRequest blockRequest)
    {
        List<RangeBlockContextImpl> blocks = new ArrayList<>();

        if (!StringUtils.isNullOrEmpty(blockRequest.getReprocessReferenceValue()))
        {
            blocks = loadRangeBlocksOfTask(blockRequest);
        }
        else
        {
            List<RangeBlockContextImpl> forceBlocks = getForcedBlocks(blockRequest);
            blocks.addAll(forceBlocks);

            if (getBlocksRemaining(blockRequest, blocks) > 0)
                loadFailedAndDeadBlocks(blockRequest, blocks);

            int blocksRemaining = getBlocksRemaining(blockRequest, blocks);
            if (blocksRemaining > 0 && blockRequest.getRangeBegin().isPresent())
                blocks.addAll(generateNewDateRangeBlocks(blockRequest, blocksRemaining));
        }

        if (blocks.isEmpty())
        {
            logEmptyBlockEvent(blockRequest.getTaskExecutionId(), blockRequest.getApplicationName(), blockRequest.getTaskName());
        }

        List<DateRangeBlockContext> dateRangeBlocks = blocks.stream().map(x -> (DateRangeBlockContext)x).collect(Collectors.toList());
        List<DateRangeBlockContext> sorted =  dateRangeBlocks.stream()
                .sorted(Comparator.comparingLong(b ->
                        Long.parseLong(b.getDateRangeBlock().getRangeBlockId())))
                .collect(Collectors.toList());

        if(blockRequest.getRangeEnd().isPresent())
            return generateDateRangeResponse(blockRequest.getRangeEnd().get(), sorted);
        else
            return new DateRangeBlockResponse(sorted);
    }

    private DateRangeBlockResponse generateDateRangeResponse(Instant rangeEnd, List<DateRangeBlockContext> blockContexts)
    {
        if(blockContexts.isEmpty())
            return new DateRangeBlockResponse(blockContexts);

        Instant maxEndDate = blockContexts.stream()
                .map(x -> x.getDateRangeBlock().getEndDate())
                .max(Instant::compareTo)
                .get();

        boolean covered = rangeEnd.equals(maxEndDate);
        if(covered) {
            return new DateRangeBlockResponse(blockContexts);
        }
        else {
            return new DateRangeBlockResponse(blockContexts, maxEndDate, rangeEnd);
        }
    }

    public NumericRangeBlockResponse generateNumericRangeBlocks(NumericRangeBlockRequest blockRequest)
    {
        List<RangeBlockContextImpl> blocks = new ArrayList<>();

        if (!StringUtils.isNullOrEmpty(blockRequest.getReprocessReferenceValue()))
        {
            blocks = loadRangeBlocksOfTask(blockRequest);
        }
        else
        {
            List<RangeBlockContextImpl> forceBlocks = getForcedBlocks(blockRequest);
            blocks.addAll(forceBlocks);

            if (getBlocksRemaining(blockRequest, blocks) > 0)
                loadFailedAndDeadBlocks(blockRequest, blocks);

            int blocksRemaining = getBlocksRemaining(blockRequest, blocks);
            if (blocksRemaining > 0 && blockRequest.getRangeBegin().isPresent())
                blocks.addAll(generateNewNumericRangeBlocks(blockRequest, blocksRemaining));
        }

        if (blocks.isEmpty())
        {
            logEmptyBlockEvent(blockRequest.getTaskExecutionId(), blockRequest.getApplicationName(), blockRequest.getTaskName());
        }

        List<NumericRangeBlockContext> numericRangeBlocks = blocks.stream().map(x -> (NumericRangeBlockContext)x).collect(Collectors.toList());
        List<NumericRangeBlockContext> sorted = numericRangeBlocks.stream()
                .sorted(Comparator.comparingLong(b ->
                        Long.parseLong(b.getNumericRangeBlock().getRangeBlockId())))
                .collect(Collectors.toList());

        if(blockRequest.getRangeEnd().isPresent())
            return generateNumericRangeResponse(blockRequest.getRangeEnd().get(), sorted);
        else
            return new NumericRangeBlockResponse(sorted);
    }

    private NumericRangeBlockResponse generateNumericRangeResponse(long rangeEnd, List<NumericRangeBlockContext> blockContexts)
    {
        if(blockContexts.isEmpty())
            return new NumericRangeBlockResponse(blockContexts);

        long maxEndNumber = blockContexts.stream()
                .map(x -> x.getNumericRangeBlock().getEndNumber())
                .max(Comparator.comparing(Long::valueOf))
                .get();

        boolean covered = rangeEnd == maxEndNumber;
        if(covered) {
            return new NumericRangeBlockResponse(blockContexts);
        }
        else {
            return new NumericRangeBlockResponse(blockContexts, maxEndNumber, rangeEnd);
        }
    }

    private int getBlocksRemaining(BlockRequest blockRequest, List<RangeBlockContextImpl> blocks)
    {
        return blockRequest.getMaxBlocks() - blocks.size();
    }

    private List<RangeBlockContextImpl> getForcedBlocks(BlockRequest blockRequest)
    {
        QueuedForcedBlocksRequest forcedBlockRequest = new QueuedForcedBlocksRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType());

        List<ForcedRangeBlockQueueItem> queuedForcedBlocks = blockRepository.getQueuedForcedRangeBlocks(forcedBlockRequest);

        List<RangeBlockContextImpl> forcedBlocks = new ArrayList<>();
        for (ForcedRangeBlockQueueItem queuedForcedBlock : queuedForcedBlocks)
            forcedBlocks.add(createBlockContext(blockRequest, queuedForcedBlock.getRangeBlock(), queuedForcedBlock.getForcedBlockQueueId()));

        if (!forcedBlocks.isEmpty()) {
            List<String> ids = forcedBlocks.stream()
                    .filter(x -> x.getForcedBlockQueueId().isPresent())
                    .map(x -> x.getForcedBlockQueueId().get())
                    .collect(Collectors.toList());
            dequeueForcedBlocks(blockRequest, ids);
        }

        return forcedBlocks;
    }

    private List<RangeBlockContextImpl> loadRangeBlocksOfTask(BlockRequest blockRequest)
    {
        FindBlocksOfTaskRequest failedBlockRequest = new FindBlocksOfTaskRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                blockRequest.getReprocessReferenceValue(),
                blockRequest.getReprocessOption());

        List<RangeBlock> blocksOfTask = blockRepository.findRangeBlocksOfTask(failedBlockRequest);
        if (blocksOfTask.isEmpty())
            return new ArrayList<>();

        return createBlockContexts(blockRequest, blocksOfTask);
    }


    private void loadFailedAndDeadBlocks(BlockRequest blockRequest, List<RangeBlockContextImpl> blocks)
    {
        int blocksRemaining = blockRequest.getMaxBlocks() - blocks.size();
        if (blockRequest.isReprocessDeadTasks())
        {
            blocks.addAll(getDeadBlocks(blockRequest, blocksRemaining));
        }

        if (getBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.isReprocessFailedTasks())
        {
            blocks.addAll(getFailedBlocks(blockRequest, blocksRemaining));
        }
    }

    private List<RangeBlockContextImpl> getDeadBlocks(BlockRequest blockRequest, int blockCountLimit)
    {
        FindDeadBlocksRequest deadBlockRequest = createDeadBlocksRequest(blockRequest, blockCountLimit);
        List<RangeBlock> deadBlocks = blockRepository.findDeadRangeBlocks(deadBlockRequest);
        return createBlockContexts(blockRequest, deadBlocks);
    }

    private List<RangeBlockContextImpl> getFailedBlocks(BlockRequest blockRequest, int blockCountLimit)
    {
        FindFailedBlocksRequest failedBlockRequest = new FindFailedBlocksRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                Instant.now().minus(blockRequest.getFailedTaskDetectionRange()),
                Instant.now(),
                blockCountLimit,
                blockRequest.getFailedTaskRetryLimit()
        );

        List<RangeBlock> failedBlocks = blockRepository.findFailedRangeBlocks(failedBlockRequest);
        return createBlockContexts(blockRequest, failedBlocks);
    }

    private List<RangeBlockContextImpl> createBlockContexts(BlockRequest blockRequest, List<RangeBlock> rangeBlocks)
    {
        List<RangeBlockContextImpl> blocks = new ArrayList<>();
        for (RangeBlock rangeBlock : rangeBlocks)
        {
            RangeBlockContextImpl blockContext = createBlockContext(blockRequest, rangeBlock, 0);
            blocks.add(blockContext);
        }

        return blocks;
    }

    private RangeBlockContextImpl createBlockContext(BlockRequest blockRequest,
                                                     RangeBlock rangeBlock,
                                                     int forcedBlockQueueId)
    {
        int attempt = rangeBlock.getAttempt() + 1;
        BlockExecutionCreateRequest createRequest = new BlockExecutionCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                rangeBlock.getRangeBlockId(),
                attempt);

        String blockExecutionId = blockRepository.addRangeBlockExecution(createRequest);
        RangeBlockContextImpl blockContext = new RangeBlockContextImpl(rangeBlockRepository,
                taskExecutionRepository,
                blockRequest.getApplicationName(),
                blockRequest.getTaskName(),
                blockRequest.getTaskExecutionId(),
                rangeBlock, blockExecutionId,
                Integer.toString(forcedBlockQueueId));

        return blockContext;
    }

    private List<RangeBlockContextImpl> generateNewDateRangeBlocks(DateRangeBlockRequest blockRequest, int blockCountLimit)
    {
        List<RangeBlockContextImpl> newBlocks = new ArrayList<>();
        Instant blockStart = blockRequest.getRangeBegin().get();
        Instant blockEnd = blockStart.plus(blockRequest.getMaxBlockRange().get());
        int blocksAdded = 0;
        boolean stopGeneration = false;

        while (blocksAdded < blockCountLimit
                && blockStart.isBefore(blockRequest.getRangeEnd().get())
                && stopGeneration == false)
        {
            if (blockEnd.isAfter(blockRequest.getRangeEnd().get()))
            {
                blockEnd = blockRequest.getRangeEnd().get();
                stopGeneration = true;
            }

            RangeBlock dateRangeBlock = generateDateRangeBlock(blockRequest, blockStart, blockEnd);
            RangeBlockContextImpl blockContext = createBlockContext(blockRequest, dateRangeBlock, 0);
            newBlocks.add(blockContext);
            blocksAdded++;

            blockStart = blockStart.plus(blockRequest.getMaxBlockRange().get());
            blockEnd = blockStart.plus(blockRequest.getMaxBlockRange().get());
        }

        return newBlocks;
    }

    private RangeBlock generateDateRangeBlock(DateRangeBlockRequest blockRequest, Instant rangeBegin, Instant rangeEnd)
    {
        RangeBlockCreateRequest request = new RangeBlockCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                rangeBegin,
                rangeEnd);

        RangeBlock rangeBlock = blockRepository.addRangeBlock(request).getBlock();
        WaitUtils.waitForMs(10); // guarantee that each block has a unique created date
        return rangeBlock;
    }

    private List<RangeBlockContextImpl> generateNewNumericRangeBlocks(NumericRangeBlockRequest blockRequest, int blockCountLimit)
    {
        List<RangeBlockContextImpl> newBlocks = new ArrayList<>();
        long blockStart = blockRequest.getRangeBegin().get();
        long blockEnd = blockStart + (blockRequest.getBlockSize().get() - 1);
        int blocksAdded = 0;
        boolean stopGeneration = false;

        while (blocksAdded < blockCountLimit
                && blockStart <= blockRequest.getRangeEnd().get()
                && stopGeneration == false)
        {
            if (blockEnd >= blockRequest.getRangeEnd().get())
            {
                blockEnd = blockRequest.getRangeEnd().get();
                stopGeneration = true;
            }

            RangeBlock numericRangeBlock = generateNumericRangeBlock(blockRequest, blockStart, blockEnd);
            RangeBlockContextImpl blockContext = createBlockContext(blockRequest, numericRangeBlock, 0);
            newBlocks.add(blockContext);
            blocksAdded++;

            blockStart = blockStart + blockRequest.getBlockSize().get();
            blockEnd = blockStart + (blockRequest.getBlockSize().get() - 1);
        }

        return newBlocks;
    }

    private RangeBlock generateNumericRangeBlock(NumericRangeBlockRequest blockRequest, long rangeBegin, long rangeEnd)
    {
        RangeBlockCreateRequest request = new RangeBlockCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                rangeBegin,
                rangeEnd);

        RangeBlock rangeBlock = blockRepository.addRangeBlock(request).getBlock();
        WaitUtils.waitForMs(10); // guarantee that each block has a unique created date
        return rangeBlock;
    }

}
