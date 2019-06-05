package com.siiconcatel.taskling.core.fluent.objectblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.fluent.FluentBlockSettingsDescriptor;
import com.siiconcatel.taskling.core.fluent.FluentBlockSettingsDescriptorImpl;
import com.siiconcatel.taskling.core.fluent.settings.ObjectBlockSettings;

public class FluentObjectBlockSettings<T> extends FluentBlockSettingsDescriptorImpl implements ObjectBlockSettings<T>
{
    private T objectData;

    public FluentObjectBlockSettings()
    {
        super(BlockType.Object);
    }

    public FluentObjectBlockSettings(T objectData)
    {
        super(BlockType.Object);
        this.objectData = objectData;
    }

    public T getObject() {
        return this.objectData;
    }

}
