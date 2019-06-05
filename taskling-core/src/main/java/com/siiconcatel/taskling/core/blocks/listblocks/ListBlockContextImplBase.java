package com.siiconcatel.taskling.core.blocks.listblocks;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.blocks.common.BlockExecutionStatus;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionChangeStatusRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.BatchUpdateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.ProtoListBlockItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks.SingleUpdateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionErrorRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.retries.RetryService;
import com.siiconcatel.taskling.core.serde.TasklingSerde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ListBlockContextImplBase<T,H> {

    protected Class<T> itemType;
    protected Class<H> headerType;
    protected ListBlockRepository listBlockRepository;
    protected TaskExecutionRepository taskExecutionRepository;
    protected String applicationName;
    protected String taskName;
    protected String taskExecutionId;
    protected int maxStatusReasonLength;
    protected Lock uncommittedListSemaphore = new ReentrantLock();
    protected Lock getItemsSemaphore = new ReentrantLock();
    protected List<ListBlockItem<T>> uncommittedItems;
    protected boolean completed;
    protected ListBlockImpl<T> headerlessBlock;
    protected ListBlockWithHeaderImpl<T, H> blockWithHeader;
    protected String blockExecutionId;
    protected ListUpdateMode listUpdateMode;
    protected int uncommittedThreshold;

    private boolean hasHeader;
    private String forcedBlockQueueId;

    public ListBlockContextImplBase(Class<T> itemType,
                                ListBlockRepository listBlockRepository,
                                TaskExecutionRepository taskExecutionRepository,
                                String applicationName,
                                String taskName,
                                String taskExecutionId,
                                ListUpdateMode listUpdateMode,
                                int uncommittedThreshold,
                                ListBlock<T> listBlock,
                                String blockExecutionId,
                                int maxStatusReasonLength,
                                String forcedBlockQueueId)
    {
        this.itemType = itemType;
        this.listBlockRepository = listBlockRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.headerlessBlock = (ListBlockImpl<T>)listBlock;
        this.blockExecutionId = blockExecutionId;
        this.listUpdateMode = listUpdateMode;
        this.forcedBlockQueueId = forcedBlockQueueId;
        this.uncommittedThreshold = uncommittedThreshold;
        this.applicationName = applicationName;
        this.taskName = taskName;
        this.taskExecutionId = taskExecutionId;
        this.maxStatusReasonLength = maxStatusReasonLength;

        if (listUpdateMode != ListUpdateMode.SingleItemCommit)
            uncommittedItems = new ArrayList<>();

        completed = false;
    }

    public ListBlockContextImplBase(Class<T> itemType,
                                    Class<H> headerType,
                                    ListBlockRepository listBlockRepository,
                                    TaskExecutionRepository taskExecutionRepository,
                                    String applicationName,
                                    String taskName,
                                    String taskExecutionId,
                                    ListUpdateMode listUpdateMode,
                                    int uncommittedThreshold,
                                    ListBlockWithHeader<T,H> listBlock,
                                    String blockExecutionId,
                                    int maxStatusReasonLength,
                                    String forcedBlockQueueId)
    {
        this.itemType = itemType;
        this.headerType = headerType;
        this.listBlockRepository = listBlockRepository;
        this.taskExecutionRepository = taskExecutionRepository;
        this.blockWithHeader = (ListBlockWithHeaderImpl<T, H>)listBlock;
        this.blockExecutionId = blockExecutionId;
        this.listUpdateMode = listUpdateMode;
        this.forcedBlockQueueId = forcedBlockQueueId;
        this.uncommittedThreshold = uncommittedThreshold;
        this.applicationName = applicationName;
        this.taskName = taskName;
        this.taskExecutionId = taskExecutionId;
        this.maxStatusReasonLength = maxStatusReasonLength;

        if (listUpdateMode != ListUpdateMode.SingleItemCommit)
            uncommittedItems = new ArrayList<>();

        completed = false;
        hasHeader = true;
    }


    public String getListBlockId()
    {
        if (hasHeader)
            return blockWithHeader.getListBlockId();

        return headerlessBlock.getListBlockId();
    }

    public Optional<String> getForcedBlockQueueId()
    {
        if(forcedBlockQueueId == null)
            return Optional.empty();

        return Optional.of(forcedBlockQueueId);
    }

    public void fillItems()
    {
        getItemsSemaphore.lock();
        try
        {
            List<ProtoListBlockItem> protoListBlockItems = listBlockRepository.getListBlockItems(
                    new TaskId(applicationName, taskName),
                    getListBlockId());
            List<ListBlockItem<T>> listBlockItems = convertToItemList(protoListBlockItems);
            setItems(listBlockItems);

            for (ListBlockItem<T> item : listBlockItems)
            {
                Consumer<ListBlockItem<T>> itemCompleteCon = rq -> this.itemComplete(rq);
                Consumer<ListBlockItemActionArgs<T>> itemFailedCon = rq -> this.itemFailed(rq);
                Consumer<ListBlockItemActionArgs<T>> discardItemCon = rq -> this.discardItem(rq);
                ((ListBlockItemImpl<T>)item).setParentContext(itemCompleteCon, itemFailedCon, discardItemCon);
            }
        }
        finally
        {
            getItemsSemaphore.unlock();
        }
    }

    public List<ListBlockItem<T>> getItems(ItemStatus... statuses)
    {
        if (hasHeader)
            return getItemsFromBlockWithHeader(statuses);

        return getItemsFromHeaderlessBlock(statuses);
    }

    public void itemComplete(ListBlockItem<T> item)
    {
        ListBlockItemImpl<T> itemImpl = (ListBlockItemImpl<T>) item;
        validateBlockIsActive();
        itemImpl.setStatus(ItemStatus.Completed);
        updateItemStatus(itemImpl);
    }

    public void itemFailed(ListBlockItem<T> item, String reason)
    {
        ListBlockItemImpl<T> itemImpl = (ListBlockItemImpl<T>) item;
        itemImpl.setStatusReason(reason);

        validateBlockIsActive();
        itemImpl.setStatus(ItemStatus.Failed);
        updateItemStatus(itemImpl);
    }

    public void discardItem(ListBlockItem<T> item, String reason)
    {
        ListBlockItemImpl<T> itemImpl = (ListBlockItemImpl<T>) item;
        itemImpl.setStatusReason(reason);

        validateBlockIsActive();
        itemImpl.setStatus(ItemStatus.Discarded);
        updateItemStatus(itemImpl);
    }

    public void start()
    {
        validateBlockIsActive();
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                BlockType.List,
                blockExecutionId,
                BlockExecutionStatus.Started);

        Consumer<BlockExecutionChangeStatusRequest> actionRequest = rq -> listBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(actionRequest, request);
    }

    public void complete()
    {
        validateBlockIsActive();
        uncommittedListSemaphore.lock();
        try
        {
            commitUncommittedItems();
        }
        finally
        {
            uncommittedListSemaphore.unlock();
        }

        BlockExecutionStatus status = BlockExecutionStatus.Completed;
        if (!getItems(ItemStatus.Failed, ItemStatus.Pending).isEmpty())
            status = BlockExecutionStatus.Failed;

        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                BlockType.List,
                blockExecutionId,
                status);

        Consumer<BlockExecutionChangeStatusRequest> actionRequest = rq -> listBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(actionRequest, request);
    }

    public void failed()
    {
        validateBlockIsActive();

        uncommittedListSemaphore.lock();
        try
        {
            commitUncommittedItems();
        }
        finally
        {
            uncommittedListSemaphore.unlock();
        }

        setStatusAsFailed();
    }

    public void failed(String message)
    {
        failed();

        String errorMessage = String.format("BlockId {0} Error: {1}", getListBlockId(), message);
        TaskExecutionErrorRequest errorRequest = new TaskExecutionErrorRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                errorMessage,
                false);

        taskExecutionRepository.error(errorRequest);
    }

    public List<T> getItemValues(ItemStatus... statuses)
    {
        if (statuses.length == 0)
            statuses = new ItemStatus[] { ItemStatus.All };

        List<T> values = new ArrayList<>();
        for(ListBlockItem<T> item : getItems(statuses))
            values.add(item.getValue());

        return values;
    }

    public void flush()
    {
        uncommittedListSemaphore.lock();
        try
        {
            commitUncommittedItems();
        }
        finally
        {
            uncommittedListSemaphore.unlock();
        }
    }

//    protected bool disposed = false;
//    protected virtual void Dispose(bool disposing)
//    {
//        if (disposed)
//            return;
//
//        if (disposing)
//        {
//            Task.Run(() => CommitUncommittedItemsAsync());
//        }
//
//        disposed = true;
//    }

    protected void validateBlockIsActive()
    {
        if (completed)
            throw new TasklingExecutionException("The block has been marked as completed");
    }

    protected void setStatusAsFailed()
    {
        BlockExecutionChangeStatusRequest request = new BlockExecutionChangeStatusRequest(
                new TaskId(applicationName, taskName),
                taskExecutionId,
                BlockType.List,
                blockExecutionId,
                BlockExecutionStatus.Failed);

        Consumer<BlockExecutionChangeStatusRequest> actionRequest = rq -> listBlockRepository.changeStatus(rq);
        RetryService.invokeWithRetry(actionRequest, request);
    }

    protected void updateItemStatus(ListBlockItem<T> item)
    {
        switch (listUpdateMode)
        {
            case SingleItemCommit:
                commit(getListBlockId(), item);
                break;
            case BatchCommitAtEnd:
                addToUncommittedItems(item);
                break;
            case PeriodicBatchCommit:
                addAndCommitIfUncommittedCountReached(item);
                break;
        }
    }

    protected void commit(String listBlockId, ListBlockItem<T> item)
    {
        SingleUpdateRequest singleUpdateRequest = new SingleUpdateRequest(
            new TaskId(applicationName, taskName),
            getListBlockId(),
            convert(item));

        Consumer<SingleUpdateRequest> actionRequest = rq -> listBlockRepository.updateListBlockItem(rq);
        RetryService.invokeWithRetry(actionRequest, singleUpdateRequest);
    }

    protected void addToUncommittedItems(ListBlockItem<T> item)
    {
        uncommittedListSemaphore.lock();
        try
        {
            uncommittedItems.add(item);
        }
        finally
        {
            uncommittedListSemaphore.unlock();
        }

    }

    protected void addAndCommitIfUncommittedCountReached(ListBlockItem<T> item)
    {
        uncommittedListSemaphore.lock();
        try
        {
            uncommittedItems.add(item);
            if (uncommittedItems.size() == uncommittedThreshold)
                commitUncommittedItems();
        }
        finally
        {
            uncommittedListSemaphore.unlock();
        }
    }

    protected void commitUncommittedItems()
    {
        List<ListBlockItem<T>> listToCommit = null;
        if (uncommittedItems != null && !uncommittedItems.isEmpty())
        {
            listToCommit = new ArrayList<ListBlockItem<T>>(uncommittedItems);
            uncommittedItems.clear();
        }

        if (listToCommit != null && !listToCommit.isEmpty())
        {
            BatchUpdateRequest batchUpdateRequest = new BatchUpdateRequest(
                    new TaskId(applicationName, taskName),
                    getListBlockId(),
                    convertToProtoList(listToCommit));

            Consumer<BatchUpdateRequest> actionRequest = rq -> listBlockRepository.batchUpdateListBlockItems(rq);
            RetryService.invokeWithRetry(actionRequest, batchUpdateRequest);
        }
    }

    protected List<ProtoListBlockItem> convertToProtoList(List<ListBlockItem<T>> listBlockItems)
    {
        List<ProtoListBlockItem> items = new ArrayList<>();

        for(ListBlockItem<T> listBlockItem : listBlockItems)
            items.add(convert(listBlockItem));

        return items;
    }

    protected ProtoListBlockItem convert(ListBlockItem<T> listBlockItem)
    {
        return new ProtoListBlockItem(
                listBlockItem.getListBlockItemId(),
                null,
                listBlockItem.getStatus(),
                limitLength(listBlockItem.getStatusReason(), maxStatusReasonLength),
                listBlockItem.getLastUpdated());
    }

    protected List<ListBlockItem<T>> convertToItemList(List<ProtoListBlockItem> listBlockItems)
    {
        List<ListBlockItem<T>> items = new ArrayList<>();

        for (ProtoListBlockItem listBlockItem : listBlockItems)
            items.add(convert(listBlockItem));

        return items;
    }

    protected ListBlockItem<T> convert(ProtoListBlockItem listBlockItem)
    {
        return new ListBlockItemImpl<T>(listBlockItem.getListBlockItemId(),
                TasklingSerde.deserialize(itemType, listBlockItem.getValue(), false),
                listBlockItem.getStatus(),
                listBlockItem.getStatusReason(),
                listBlockItem.getLastUpdated());
    }

    protected String limitLength(String input, int limit)
    {
        if (input == null)
            return null;

        if (limit < 1)
            return input;

        if (input.length() > limit)
            return input.substring(0, limit);

        return input;
    }

    private void setItems(List<ListBlockItem<T>> items)
    {
        if (hasHeader)
            blockWithHeader.setItems(items);
        else
            headerlessBlock.setItems(items);
    }

    private List<ListBlockItem<T>> getItemsFromHeaderlessBlock(ItemStatus... statuses)
    {
        if (statuses.length== 0)
            statuses = new ItemStatus[] { ItemStatus.All };

        getItemsSemaphore.lock();
        try
        {
            if (headerlessBlock.getItems() == null || headerlessBlock.getItems().isEmpty())
            {
                List<ProtoListBlockItem> protoListBlockItems = listBlockRepository.getListBlockItems(
                        new TaskId(applicationName, taskName),
                        getListBlockId());
                headerlessBlock.setItems(convertToItemList(protoListBlockItems));

                for (ListBlockItem<T> item : headerlessBlock.getItems())
                {
                    Consumer<ListBlockItem<T>> itemCompleteCon = rq -> this.itemComplete(rq);
                    Consumer<ListBlockItemActionArgs<T>> itemFailedCon = rq -> this.itemFailed(rq);
                    Consumer<ListBlockItemActionArgs<T>> discardItemCon = rq -> this.discardItem(rq);
                    ((ListBlockItemImpl<T>)item).setParentContext(itemCompleteCon, itemFailedCon, discardItemCon);
                }
            }

            if (Arrays.stream(statuses).anyMatch(x -> x == ItemStatus.All)) {
                return headerlessBlock.getItems()
                        .stream()
                        .filter(x -> x.getStatus() == ItemStatus.Failed
                                || x.getStatus() == ItemStatus.Pending
                                || x.getStatus() == ItemStatus.Discarded
                                || x.getStatus() == ItemStatus.Completed)
                        .collect(Collectors.toList());
            }

            List<ItemStatus> statusList = Arrays.asList(statuses);
            return headerlessBlock.getItems()
                    .stream()
                    .filter(x -> statusList.contains(x.getStatus()))
                    .collect(Collectors.toList());
        }
        finally
        {
            getItemsSemaphore.unlock();
        }
    }

    private List<ListBlockItem<T>> getItemsFromBlockWithHeader(ItemStatus... statuses)
    {
        if (statuses.length == 0)
            statuses = new ItemStatus[] { ItemStatus.All };

        getItemsSemaphore.lock();
        try
        {
            if (blockWithHeader.getItems() == null || blockWithHeader.getItems().isEmpty())
            {
                List<ProtoListBlockItem> protoListBlockItems = listBlockRepository.getListBlockItems(
                        new TaskId(applicationName, taskName),
                        getListBlockId());
                blockWithHeader.setItems(convertToItemList(protoListBlockItems));

                for (ListBlockItem<T> item : blockWithHeader.getItems())
                {
                    Consumer<ListBlockItem<T>> itemCompleteCon = rq -> this.itemComplete(rq);
                    Consumer<ListBlockItemActionArgs<T>> itemFailedCon = rq -> this.itemFailed(rq);
                    Consumer<ListBlockItemActionArgs<T>> discardItemCon = rq -> this.discardItem(rq);
                    ((ListBlockItemImpl<T>)item).setParentContext(itemCompleteCon, itemFailedCon, discardItemCon);
                }
            }

            if (Arrays.stream(statuses).anyMatch(x -> x == ItemStatus.All))
                return blockWithHeader.getItems()
                        .stream()
                        .filter(x -> x.getStatus() == ItemStatus.Failed
                                || x.getStatus() == ItemStatus.Pending
                                || x.getStatus() == ItemStatus.Discarded
                                || x.getStatus() == ItemStatus.Completed)
                        .collect(Collectors.toList());

            List<ItemStatus> statusList = Arrays.asList(statuses);
            return blockWithHeader.getItems()
                    .stream()
                    .filter(x -> statusList.contains(x.getStatus()))
                    .collect(Collectors.toList());
        }
        finally
        {
            getItemsSemaphore.unlock();
        }
    }

    private void itemFailed(ListBlockItemActionArgs actionArgs) {
        itemFailed(actionArgs.getListBlockItem(), actionArgs.getMessage());
    }

    private void discardItem(ListBlockItemActionArgs actionArgs) {
        discardItem(actionArgs.getListBlockItem(), actionArgs.getMessage());
    }

//    public void Dispose()
//    {
//        Dispose(true);
//    }
}
