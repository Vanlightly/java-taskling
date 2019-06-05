package com.siiconcatel.taskling.core.fluent.rangeblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.fluent.FluentBlockSettingsDescriptor;
import com.siiconcatel.taskling.core.fluent.FluentBlockSettingsDescriptorImpl;
import com.siiconcatel.taskling.core.fluent.OverrideConfigurationDescriptor;
import com.siiconcatel.taskling.core.fluent.ReprocessScopeDescriptor;

import java.time.Duration;
import java.time.Instant;

public class FluentRangeBlockDescriptor implements FluentDateRangeBlockDescriptor, FluentNumericRangeBlockDescriptor {
    public ReprocessScopeDescriptor reprocessDateRange()
    {
        return new FluentBlockSettingsDescriptorImpl(BlockType.DateRange);
    }

    public ReprocessScopeDescriptor reprocessNumericRange()
    {
        return new FluentBlockSettingsDescriptorImpl(BlockType.NumericRange);
    }

    public OverrideConfigurationDescriptor withRange(Instant fromDate, Instant toDate, Duration maxBlockRange)
    {
        return new FluentBlockSettingsDescriptorImpl(fromDate, toDate, maxBlockRange);
    }

    public OverrideConfigurationDescriptor withRange(long fromNumber, long toNumber, long maxBlockNumberRange)
    {
        return new FluentBlockSettingsDescriptorImpl(fromNumber, toNumber, maxBlockNumberRange);
    }

    public OverrideConfigurationDescriptor onlyOldDateBlocks()
    {
        return new FluentBlockSettingsDescriptorImpl(BlockType.DateRange);
    }

    public OverrideConfigurationDescriptor onlyOldNumericBlocks()
    {
        return new FluentBlockSettingsDescriptorImpl(BlockType.NumericRange);
    }
}
