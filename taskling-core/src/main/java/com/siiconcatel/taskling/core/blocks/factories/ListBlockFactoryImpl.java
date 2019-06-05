package com.siiconcatel.taskling.core.blocks.factories;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.*;
import com.siiconcatel.taskling.core.blocks.requests.*;
import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.contexts.ListBlockWithHeaderContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindBlocksOfTaskRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindDeadBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindFailedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.ForcedListBlockQueueItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.QueuedForcedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ListBlockCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlockItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.utils.Pair;
import com.siiconcatel.taskling.core.utils.StringUtils;
import com.siiconcatel.taskling.core.utils.WaitUtils;
import com.siiconcatel.taskling.core.serde.TasklingSerde;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ListBlockFactoryImpl extends BlockFactoryBase implements ListBlockFactory {

    private final ListBlockRepository listBlockRepository;

    public ListBlockFactoryImpl(BlockRepository blockRepository,
                                TaskExecutionRepository taskExecutionRepository,
                                ListBlockRepository listBlockRepository)
    {
        super(taskExecutionRepository, blockRepository);
        this.listBlockRepository = listBlockRepository;
    }

    public <T> ListBlockResponse<T> generateListBlocks(ItemOnlyListBlockRequest<T> blockRequest)
    {
        Pair<List<ProtoListBlock>,List<String>> blocks = createProtoListBlocks(blockRequest);
        List<ListBlockContext<T>> blockContexts = createListBlockContexts(blockRequest, blocks.Item1);

        List<String> forcedBlocksIds = blocks.Item1.stream()
                .filter(x -> x.isForcedBlock())
                .map(x -> String.valueOf(x.getForcedBlockQueueId()))
                .collect(Collectors.toList());

        if (!forcedBlocksIds.isEmpty())
            dequeueForcedBlocks(blockRequest, forcedBlocksIds);

        if (blocks.Item1.isEmpty())
        {
            logEmptyBlockEvent(blockRequest.getTaskExecutionId(), blockRequest.getApplicationName(), blockRequest.getTaskName());
        }

        List<ListBlockContext<T>> sorted =  blockContexts.stream()
                .sorted(Comparator.comparingLong(b -> Long.parseLong(b.getListBlockId())))
                .collect(Collectors.toList());

        if(blocks.Item2.isEmpty())
            return new ListBlockResponse<T>(sorted);
        else
            return new ListBlockResponse<T>(sorted, TasklingSerde.deserialize(blockRequest.getItemType(), blocks.Item2, false));
    }

    public <T,H> ListBlockWithHeaderResponse<T,H> generateListBlocksWithHeader(ItemHeaderListBlockRequest<T,H> blockRequest)
    {
        Pair<List<ProtoListBlock>,List<String>> blocks = createProtoListBlocks(blockRequest);
        List<ListBlockWithHeaderContext<T,H>> blockContexts = createListBlockContexts(blockRequest, blocks.Item1);

        List<String> forcedBlockIds = blocks.Item1.stream()
                .filter(x -> x.isForcedBlock())
                .map(x -> String.valueOf(x.getForcedBlockQueueId()))
                .collect(Collectors.toList());

        if (!forcedBlockIds.isEmpty())
            dequeueForcedBlocks(blockRequest, forcedBlockIds);

        if (blocks.Item1.isEmpty()) {
            logEmptyBlockEvent(blockRequest.getTaskExecutionId(),
                    blockRequest.getApplicationName(),
                    blockRequest.getTaskName());
        }

        List<ListBlockWithHeaderContext<T,H>> sorted = blockContexts.stream()
                .sorted(Comparator.comparing(x -> Long.parseLong(x.getListBlockId())))
                .collect(Collectors.toList());

        if(blocks.Item2.isEmpty())
            return new ListBlockWithHeaderResponse<T,H>(sorted);
        else
            return new ListBlockWithHeaderResponse<T,H>(sorted, TasklingSerde.deserialize(blockRequest.getItemType(), blocks.Item2, false));
    }

    public <T> ListBlock<T> getLastListBlock(ItemOnlyLastBlockRequest<T> lastBlockRequest)
    {
        ProtoListBlock lastProtoListBlock = listBlockRepository.getLastListBlock(lastBlockRequest);
        if (lastProtoListBlock == null)
            return null;

        return convert(lastBlockRequest.getItemType(), lastProtoListBlock, true);
    }

    public <T,H> ListBlockWithHeader<T, H> getLastListBlockWithHeader(ItemHeaderLastBlockRequest<T,H> lastBlockRequest)
    {
        ProtoListBlock lastProtoListBlock = listBlockRepository.getLastListBlock(lastBlockRequest);

        return convert(lastBlockRequest.getItemType(),
                lastBlockRequest.getHeaderType(),
                lastProtoListBlock,
                true);
    }

    private <T> Pair<List<ProtoListBlock>, List<String>> createProtoListBlocks(ListBlockRequest blockRequest)
    {
        List<ProtoListBlock> blocks = new ArrayList<>();
        List<String> leftOverValues = new ArrayList<>();

        if (!StringUtils.isNullOrEmpty(blockRequest.getReprocessReferenceValue()))
        {
            blocks = loadListBlocksOfTask(blockRequest);
        }
        else
        {
            // Forced blocks
            List<ForcedListBlockQueueItem> forceBlockQueueItems = getForcedListBlocks(blockRequest);
            List<ProtoListBlock> forceBlocks = new ArrayList<>();
            for (ForcedListBlockQueueItem forceBlockQueueItem : forceBlockQueueItems)
            {
                ProtoListBlock forceBlock = forceBlockQueueItem.getListBlock();
                forceBlock.setForcedBlock(true);
                forceBlock.setForcedBlockQueueId(forceBlockQueueItem.getForcedBlockQueueId());
                forceBlocks.add(forceBlock);
            }

            blocks.addAll(forceBlocks);

            // Failed and Dead blocks
            if (getBlocksRemaining(blockRequest, blocks) > 0)
                loadFailedAndDeadListBlocks(blockRequest, blocks);

            // New blocks
            int blocksRemaining = getBlocksRemaining(blockRequest, blocks);
            if (blocksRemaining > 0 &&
                    blockRequest.getSerializedValues() != null
                    && !blockRequest.getSerializedValues().isEmpty())
            {
                Pair<List<ProtoListBlock>,List<String>> newBlocks = generateNewListBlocks(blockRequest, blocksRemaining);
                blocks.addAll(newBlocks.Item1);
                leftOverValues = newBlocks.Item2;
            }
        }

        return new Pair<>(blocks, leftOverValues);
    }

    private int getBlocksRemaining(ListBlockRequest blockRequest, List<ProtoListBlock> blocks)
    {
        return blockRequest.getMaxBlocks() - blocks.size();
    }

    private List<ProtoListBlock> loadListBlocksOfTask(ListBlockRequest blockRequest)
    {
        FindBlocksOfTaskRequest failedBlockRequest = new FindBlocksOfTaskRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                blockRequest.getReprocessReferenceValue(),
                blockRequest.getReprocessOption());

        List<ProtoListBlock> blocksOfTask = blockRepository.findListBlocksOfTask(failedBlockRequest);

        return blocksOfTask;
    }

    private List<ForcedListBlockQueueItem> getForcedListBlocks(ListBlockRequest blockRequest)
    {
        QueuedForcedBlocksRequest forcedBlockRequest = new QueuedForcedBlocksRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType());

        List<ForcedListBlockQueueItem> queuedForcedBlocks = blockRepository.getQueuedForcedListBlocks(forcedBlockRequest);

        return queuedForcedBlocks;
    }

    private void loadFailedAndDeadListBlocks(ListBlockRequest blockRequest, List<ProtoListBlock> blocks)
    {
        int blocksRemaining = getBlocksRemaining(blockRequest, blocks);
        if (blockRequest.isReprocessDeadTasks())
        {
            blocks.addAll(getDeadListBlocks(blockRequest, blocksRemaining));
            blocksRemaining = blockRequest.getMaxBlocks() - blocks.size();
        }

        if (getBlocksRemaining(blockRequest, blocks) > 0
                && blockRequest.isReprocessFailedTasks())
        {
            blocks.addAll(getFailedListBlocks(blockRequest, blocksRemaining));
        }
    }

    private List<ProtoListBlock> getDeadListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
    {
        FindDeadBlocksRequest deadBlockRequest = createDeadBlocksRequest(blockRequest, blockCountLimit);
        List<ProtoListBlock> deadBlocks = blockRepository.findDeadListBlocks(deadBlockRequest);

        return deadBlocks;
    }

    private List<ProtoListBlock> getFailedListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
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

        List<ProtoListBlock> failedBlocks = blockRepository.findFailedListBlocks(failedBlockRequest);
        return failedBlocks;
    }

    private Pair<List<ProtoListBlock>, List<String>> generateNewListBlocks(ListBlockRequest blockRequest, int blockCountLimit)
    {
        List<ProtoListBlock> newBlocks = new ArrayList<>();
        List<String> leftOverValues = new ArrayList<>();

        int listLength = blockRequest.getSerializedValues().size();
        int listIndex = 0;
        int blocksAdded = 0;

        List<String> values = new ArrayList<>();

        while (listIndex < listLength)
        {
            String currValue = blockRequest.getSerializedValues().get(listIndex);

            if(blocksAdded < blockCountLimit) {
                values.add(currValue);

                if (values.size() == blockRequest.getMaxBlockSize() || listIndex == listLength - 1) {
                    ProtoListBlock newListBlock = generateListBlock(blockRequest, values);
                    newBlocks.add(newListBlock);
                    values = new ArrayList<>();
                    blocksAdded++;
                }
            }
            else {
                leftOverValues.add(currValue);
            }

            listIndex++;
        }

        return new Pair<>(newBlocks, leftOverValues);
    }

    private ProtoListBlock generateListBlock(ListBlockRequest blockRequest, List<String> values)
    {
        ListBlockCreateRequest request = new ListBlockCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                values,
                blockRequest.getSerializedHeader(),
                blockRequest.getCompressionThreshold());

        ProtoListBlock listBlock = blockRepository.addListBlock(request).getBlock();
        WaitUtils.waitForMs(10); // guarantee that each block has a unique created date
        return listBlock;
    }

    private <T> List<ListBlockContext<T>> createListBlockContexts(ItemOnlyListBlockRequest<T> blockRequest,
                                                                  List<ProtoListBlock> listBlocks)
    {
        List<ListBlockContext<T>> blocks = new ArrayList<>();
        for (ProtoListBlock listBlock : listBlocks)
        {
            ListBlockContext<T> blockContext = createListBlockContext(blockRequest, listBlock, 0);
            blocks.add(blockContext);
        }

        return blocks;
    }

    private <T> ListBlockContext<T> createListBlockContext(ItemOnlyListBlockRequest<T> blockRequest,
                                                           ProtoListBlock listBlock,
                                                           int forcedBlockQueueId) {
        int attempt = listBlock.getAttempt() + 1;
        BlockExecutionCreateRequest createRequest = new BlockExecutionCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                BlockType.List,
                listBlock.getListBlockId(),
                attempt);

        String blockExecutionId = blockRepository.addListBlockExecution(createRequest);

        ListBlock<T> listBlockOfT = convert(blockRequest.getItemType(), listBlock, false);
        ListBlockContext<T> blockContext = new ListBlockContextImpl<T>(
                blockRequest.getItemType(),
                listBlockRepository,
                taskExecutionRepository,
                blockRequest.getApplicationName(),
                blockRequest.getTaskName(),
                blockRequest.getTaskExecutionId(),
                blockRequest.getListUpdateMode(),
                blockRequest.getUncommittedItemsThreshold(),
                listBlockOfT,
                blockExecutionId,
                blockRequest.getMaxStatusReasonLength(),
                Integer.toString(forcedBlockQueueId));

        return blockContext;
    }

    private <T,H> List<ListBlockWithHeaderContext<T, H>> createListBlockContexts(ItemHeaderListBlockRequest<T,H> blockRequest,
                                                                                 List<ProtoListBlock> listBlocks) {
        List<ListBlockWithHeaderContext<T, H>> blocks = new ArrayList<>();
        for (ProtoListBlock listBlock : listBlocks)
        {
            ListBlockWithHeaderContext<T,H> blockContext = createListBlockContext(blockRequest, listBlock, 0);
            blocks.add(blockContext);
        }

        return blocks;
    }

    private <T,H> ListBlockWithHeaderContext<T,H> createListBlockContext(ItemHeaderListBlockRequest<T,H> blockRequest,
                                                               ProtoListBlock listBlock,
                                                               int forcedBlockQueueId) {
        int attempt = listBlock.getAttempt() + 1;
        BlockExecutionCreateRequest createRequest = new BlockExecutionCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                listBlock.getListBlockId(),
                attempt);

        String blockExecutionId = blockRepository.addListBlockExecution(createRequest);

        ListBlockWithHeader<T,H> listBlockOfT = convert(blockRequest.getItemType(),
                blockRequest.getHeaderType(),
                listBlock,
                false);

        ListBlockWithHeaderContext<T,H> blockContext = new ListBlockWithHeaderContextImpl<T,H>(
                blockRequest.getItemType(),
                blockRequest.getHeaderType(),
                listBlockRepository,
                taskExecutionRepository,
                blockRequest.getApplicationName(),
                blockRequest.getTaskName(),
                blockRequest.getTaskExecutionId(),
                blockRequest.getListUpdateMode(),
                blockRequest.getUncommittedItemsThreshold(),
                listBlockOfT,
                blockExecutionId,
                blockRequest.getMaxStatusReasonLength(),
                String.valueOf(forcedBlockQueueId));

        return blockContext;
    }

    private <T> ListBlock<T> convert(Class<T> itemType, ProtoListBlock protoListBlock, boolean fillBlock) {
        if (protoListBlock == null)
            return null;

        ListBlockImpl<T> block = new ListBlockImpl<T>();
        block.setAttempt(protoListBlock.getAttempt());

        if (fillBlock)
            block.setItems(convert(itemType, protoListBlock.getItems()));

        block.setListBlockId(protoListBlock.getListBlockId());

        return block;
    }

    private <T,H> ListBlockWithHeader<T, H> convert(Class<T> itemType,
                                                    Class<H> headerType,
                                                    ProtoListBlock protoListBlock,
                                                    boolean fillBlock) {
        if (protoListBlock == null)
            return null;

        ListBlockWithHeaderImpl<T, H> block = new ListBlockWithHeaderImpl<T, H>();
        block.setAttempt(protoListBlock.getAttempt());

        if (fillBlock)
            block.setItems(convert(itemType, protoListBlock.getItems()));

        block.setListBlockId(protoListBlock.getListBlockId());
        block.setHeader(TasklingSerde.deserialize(headerType, protoListBlock.getHeader(), false));

        return block;
    }

    private <T> List<ListBlockItem<T>> convert(Class<T> itemType,
                                               List<ProtoListBlockItem> protoListBlockItems) {
        if (protoListBlockItems == null)
            return null;

        List<ListBlockItem<T>> items = new ArrayList<>();

        for (ProtoListBlockItem protoItem : protoListBlockItems)
        {
            ListBlockItem<T> item = new ListBlockItemImpl<T>(protoItem.getListBlockItemId(),
                    TasklingSerde.deserialize(itemType, protoItem.getValue(), false),
                    protoItem.getStatus(),
                    protoItem.getStatusReason(),
                    protoItem.getLastUpdated());

            items.add(item);
        }

        return items;
    }
}
