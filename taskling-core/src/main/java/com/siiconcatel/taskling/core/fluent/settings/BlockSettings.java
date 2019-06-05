package com.siiconcatel.taskling.core.fluent.settings;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.ListUpdateMode;
import com.siiconcatel.taskling.core.tasks.ReprocessOption;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface BlockSettings
{
    BlockType getBlockType();

    // DateRange
    Optional<Instant> getFromDate();
    Optional<Instant> getToDate();
    Optional<Duration> getMaxBlockTimespan();

    // NumericRange
    Optional<Long> getFromNumber();
    Optional<Long> getToNumber();
    Optional<Long> getMaxBlockNumberRange();

    // ListBlocks
    List<String> getValues();
    String getHeader();
    short getMaxBlockSize();
    ListUpdateMode getListUpdateMode();
    int getUncommittedItemsThreshold();

    // Reprocess Specific Task
    ReprocessOption getReprocessOption();
    String getReferenceValueToReprocess();

    // Configuration Overridable
    Optional<Boolean> getMustReprocessFailedTasks();
    Optional<Duration> getFailedTaskDetectionRange();
    Optional<Short> getFailedTaskRetryLimit();
    Optional<Boolean> getMustReprocessDeadTasks();
    Optional<Duration> getDeadTaskDetectionRange();
    Optional<Short> getDeadTaskRetryLimit();
    Optional<Integer> getMaximumNumberOfBlocksLimit();
}
