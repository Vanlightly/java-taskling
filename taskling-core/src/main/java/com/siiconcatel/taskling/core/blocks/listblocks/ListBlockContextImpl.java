package com.siiconcatel.taskling.core.blocks.listblocks;

import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;

import java.util.Optional;

public class ListBlockContextImpl<T> extends ListBlockContextImplBase<T, String> implements ListBlockContext<T>  {


    public ListBlockContextImpl(Class<T> itemType,
                                ListBlockRepository listBlockRepository,
                                TaskExecutionRepository taskExecutionRepository,
                                String applicationName,
                                String taskName,
                                String taskExecutionId,
                                ListUpdateMode listUpdateMode,
                                int uncommittedThreshold,
                                ListBlock<T> listBlock,
                                String blockExecutionId,
                                int maxStatusReasonLength)
    {
        super(itemType,
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
        headerlessBlock.setParentContext(this);
    }

    public ListBlockContextImpl(Class<T> itemType,
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
        super(itemType,
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
        headerlessBlock.setParentContext(this);
    }

    @Override
    public ListBlock<T> getBlock() {
        return headerlessBlock;
    }
}
