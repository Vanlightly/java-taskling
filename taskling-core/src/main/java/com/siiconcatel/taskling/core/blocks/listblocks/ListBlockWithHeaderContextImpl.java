package com.siiconcatel.taskling.core.blocks.listblocks;

import com.siiconcatel.taskling.core.contexts.ListBlockWithHeaderContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;

import java.util.Optional;

public class ListBlockWithHeaderContextImpl<T,H>  extends ListBlockContextImplBase<T, H> implements ListBlockWithHeaderContext<T,H> {


    public ListBlockWithHeaderContextImpl(Class<T> itemType,
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
                                          int maxStatusReasonLength)
    {
        super(itemType,
                headerType,
                listBlockRepository,
                taskExecutionRepository,
                applicationName,
                taskName,
                taskExecutionId,
                listUpdateMode,
                uncommittedThreshold,
                listBlock,
                blockExecutionId,
                maxStatusReasonLength,
                null);
        blockWithHeader.setParentContext(this);
    }

    public ListBlockWithHeaderContextImpl(Class<T> itemType,
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
        super(itemType,
                headerType,
                listBlockRepository,
                taskExecutionRepository,
                applicationName,
                taskName,
                taskExecutionId,
                listUpdateMode,
                uncommittedThreshold,
                listBlock,
                blockExecutionId,
                maxStatusReasonLength,
                forcedBlockQueueId);
        blockWithHeader.setParentContext(this);
    }

    @Override
    public ListBlockWithHeader<T,H> getBlock() {
        return blockWithHeader;
    }
}
