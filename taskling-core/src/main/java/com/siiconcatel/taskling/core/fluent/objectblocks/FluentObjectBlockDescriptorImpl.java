package com.siiconcatel.taskling.core.fluent.objectblocks;

import com.siiconcatel.taskling.core.fluent.OverrideConfigurationDescriptor;
import com.siiconcatel.taskling.core.fluent.ReprocessScopeDescriptor;

public class FluentObjectBlockDescriptorImpl<T> implements FluentObjectBlockDescriptor<T> {

    public OverrideConfigurationDescriptor withObject(T data)
    {
        FluentObjectBlockSettings stringBlockDescriptor = new FluentObjectBlockSettings<T>(data);

        return stringBlockDescriptor;
    }

    public OverrideConfigurationDescriptor withNoNewBlocks()
    {
        return new FluentObjectBlockSettings<T>();
    }

    public ReprocessScopeDescriptor reprocess()
    {
        return new FluentObjectBlockSettings<T>();
    }
}
