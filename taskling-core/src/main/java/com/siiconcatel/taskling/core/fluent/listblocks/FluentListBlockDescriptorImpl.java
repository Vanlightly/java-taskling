package com.siiconcatel.taskling.core.fluent.listblocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.BatchSize;
import com.siiconcatel.taskling.core.blocks.listblocks.ListUpdateMode;
import com.siiconcatel.taskling.core.fluent.FluentBlockSettingsDescriptorImpl;
import com.siiconcatel.taskling.core.fluent.OverrideConfigurationDescriptor;
import com.siiconcatel.taskling.core.fluent.ReprocessScopeDescriptor;
import com.siiconcatel.taskling.core.serde.TasklingSerde;

import java.util.ArrayList;
import java.util.List;

public class FluentListBlockDescriptorImpl<T> implements FluentListBlockDescriptor<T> {

    private ObjectMapper mapper;

    public FluentListBlockDescriptorImpl() {
         this.mapper = new ObjectMapper();
    }

    @Override
    public OverrideConfigurationDescriptor withSingleUnitCommit(List<T> values, short maxBlockSize)
    {
        List<String> jsonValues = serialize(values);
        FluentBlockSettingsDescriptorImpl listBlockDescriptor = new FluentBlockSettingsDescriptorImpl(jsonValues, maxBlockSize);
        listBlockDescriptor.setListUpdateMode(ListUpdateMode.SingleItemCommit);

        return listBlockDescriptor;
    }

    @Override
    public OverrideConfigurationDescriptor withPeriodicCommit(List<T> values, short maxBlockSize, BatchSize batchSize)
    {
        List<String> jsonValues = serialize(values);
        FluentBlockSettingsDescriptorImpl listBlockDescriptor = new FluentBlockSettingsDescriptorImpl(jsonValues, maxBlockSize);
        listBlockDescriptor.setListUpdateMode(ListUpdateMode.PeriodicBatchCommit);

        switch (batchSize)
        {
            case NotSet:
                listBlockDescriptor.setUncommittedItemsThreshold(100);
                break;
            case Ten:
                listBlockDescriptor.setUncommittedItemsThreshold(10);
                break;
            case Fifty:
                listBlockDescriptor.setUncommittedItemsThreshold(50);
                break;
            case Hundred:
                listBlockDescriptor.setUncommittedItemsThreshold(100);
                break;
            case FiveHundred:
                listBlockDescriptor.setUncommittedItemsThreshold(500);
                break;
        }

        return listBlockDescriptor;
    }

    @Override
    public OverrideConfigurationDescriptor withBatchCommitAtEnd(List<T> values, short maxBlockSize)
    {
        List<String> jsonValues = serialize(values);
        FluentBlockSettingsDescriptorImpl listBlockDescriptor = new FluentBlockSettingsDescriptorImpl(jsonValues, maxBlockSize);
        listBlockDescriptor.setListUpdateMode(ListUpdateMode.BatchCommitAtEnd);

        return listBlockDescriptor;
    }

    @Override
    public ReprocessScopeDescriptor reprocessWithSingleUnitCommit()
    {
        FluentBlockSettingsDescriptorImpl listBlockDescriptor = new FluentBlockSettingsDescriptorImpl(BlockType.List);
        listBlockDescriptor.setListUpdateMode(ListUpdateMode.SingleItemCommit);

        return listBlockDescriptor;
    }

    @Override
    public ReprocessScopeDescriptor reprocessWithPeriodicCommit(BatchSize batchSize)
    {
        FluentBlockSettingsDescriptorImpl listBlockDescriptor = new FluentBlockSettingsDescriptorImpl(BlockType.List);
        listBlockDescriptor.setListUpdateMode(ListUpdateMode.PeriodicBatchCommit);

        return listBlockDescriptor;
    }

    @Override
    public ReprocessScopeDescriptor reprocessWithBatchCommitAtEnd()
    {
        FluentBlockSettingsDescriptorImpl listBlockDescriptor = new FluentBlockSettingsDescriptorImpl(BlockType.List);
        listBlockDescriptor.setListUpdateMode(ListUpdateMode.BatchCommitAtEnd);

        return listBlockDescriptor;
    }

    private List<String> serialize(List<T> values)
    {
        List<String> jsonValues = new ArrayList<>();
        for(T value : values)
        {
            jsonValues.add(TasklingSerde.serialize(value, false));
        }

        return jsonValues;
    }
}
