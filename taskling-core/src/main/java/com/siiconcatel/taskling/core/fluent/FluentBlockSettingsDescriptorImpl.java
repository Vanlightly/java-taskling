package com.siiconcatel.taskling.core.fluent;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.listblocks.ListUpdateMode;
import com.siiconcatel.taskling.core.fluent.settings.BlockSettings;
import com.siiconcatel.taskling.core.tasks.ReprocessOption;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class FluentBlockSettingsDescriptorImpl implements FluentBlockSettingsDescriptor, OverrideConfigurationDescriptor, ReprocessScopeDescriptor, ReprocessTaskDescriptor, BlockSettings, CompleteDescriptor {
    private Boolean mustReprocessFailedTasks;
    private Duration failedTaskDetectionRange;
    private Short failedTaskRetryLimit;

    private Boolean mustReprocessDeadTasks;
    private Duration deadTaskDetectionRange;
    private Short deadTaskRetryLimit;

    private Integer maximumNumberOfBlocksLimit;

    private TaskDeathMode taskDeathMode;
    private BlockType blockType;

    // Date Range
    private Instant fromDate;
    private Instant toDate;
    private Duration maxBlockTimespan;

    // Numeric Range
    public Long fromNumber;
    public Long toNumber;
    public Long maxBlockNumberRange;

    // ListBlocks
    private List<String> values;
    private String header;
    private short maxBlockSize;
    private ListUpdateMode listUpdateMode;
    private int uncommittedItemsThreshold;

    // Reprocess Specific Task
    private ReprocessOption reprocessOption;
    private String referenceValueToReprocess;


    public FluentBlockSettingsDescriptorImpl(BlockType blockType)
    {
        this.blockType = blockType;
    }

    public FluentBlockSettingsDescriptorImpl(Instant fromDate, Instant toDate, Duration maxBlockRange)
    {
        this.fromDate = fromDate;
        this.toDate = toDate;
        this.maxBlockTimespan = maxBlockRange;
        this.blockType = BlockType.DateRange;
    }

    public FluentBlockSettingsDescriptorImpl(long fromNumber, long toNumber, long maxBlockRange)
    {
        this.fromNumber = fromNumber;
        this.toNumber = toNumber;
        this.maxBlockNumberRange = maxBlockRange;
        this.blockType = BlockType.NumericRange;
    }

    public FluentBlockSettingsDescriptorImpl(List<String> values, short maxBlockSize)
    {
        this.values = values;
        this.maxBlockSize = maxBlockSize;
        this.blockType = BlockType.List;
    }

    public FluentBlockSettingsDescriptorImpl(List<String> values, String header, short maxBlockSize)
    {
        this.values = values;
        this.header = header;
        this.maxBlockSize = maxBlockSize;
        this.blockType = BlockType.List;
    }



    public FluentBlockSettingsDescriptor reprocessFailedTasks(Duration detectionRange, short retryLimit)
    {
        this.mustReprocessFailedTasks = true;
        this.failedTaskDetectionRange = detectionRange;
        this.failedTaskRetryLimit = retryLimit;
        return this;
    }

    public FluentBlockSettingsDescriptor reprocessDeadTasks(Duration detectionRange, short retryLimit)
    {
        this.mustReprocessDeadTasks = true;
        this.deadTaskDetectionRange = detectionRange;
        this.deadTaskRetryLimit = retryLimit;
        return this;
    }

    public CompleteDescriptor maximumBlocksToGenerate(int maximumNumberOfBlocks)
    {
        this.maximumNumberOfBlocksLimit = maximumNumberOfBlocks;
        return this;
    }

    public CompleteDescriptor ofExecutionWith(String referenceValue)
    {
        this.referenceValueToReprocess = referenceValue;
        return this;
    }

    public ReprocessTaskDescriptor allBlocks()
    {
        this.reprocessOption = ReprocessOption.Everything;
        return this;
    }

    public ReprocessTaskDescriptor pendingAndFailedBlocks()
    {
        this.reprocessOption = ReprocessOption.PendingOrFailed;
        return this;
    }

    public FluentBlockSettingsDescriptor overrideConfiguration()
    {
        return this;
    }

    @Override
    public BlockType getBlockType() {
        return this.blockType;
    }

    @Override
    public Optional<Instant> getFromDate() {
        if(this.fromDate == null)
            return Optional.empty();

        return Optional.of(this.fromDate);
    }

    @Override
    public Optional<Instant> getToDate() {
        if(this.toDate == null)
            return Optional.empty();

        return Optional.of(this.toDate);
    }

    @Override
    public Optional<Duration> getMaxBlockTimespan() {
        if(this.maxBlockTimespan == null)
            return Optional.empty();

        return Optional.of(this.maxBlockTimespan);
    }

    @Override
    public Optional<Long> getFromNumber() {
        if(this.fromNumber == null)
            return Optional.empty();
        return Optional.of(this.fromNumber);
    }

    @Override
    public Optional<Long> getToNumber() {
        if(this.toNumber == null)
            return Optional.empty();
        return Optional.of(this.toNumber);
    }

    @Override
    public Optional<Long> getMaxBlockNumberRange() {
        if(this.maxBlockNumberRange == null)
            return Optional.empty();
        return Optional.of(this.maxBlockNumberRange);
    }

    @Override
    public List<String> getValues() {
        return this.values;
    }

    @Override
    public String getHeader() {
        return this.header;
    }

    @Override
    public short getMaxBlockSize() {
        return this.maxBlockSize;
    }

    @Override
    public ListUpdateMode getListUpdateMode() {
        return this.listUpdateMode;
    }

    @Override
    public int getUncommittedItemsThreshold() {
        return this.uncommittedItemsThreshold;
    }

    @Override
    public ReprocessOption getReprocessOption() {
        return this.reprocessOption;
    }

    @Override
    public String getReferenceValueToReprocess() {
        return this.referenceValueToReprocess;
    }

    @Override
    public Optional<Boolean> getMustReprocessFailedTasks() {
        if(this.mustReprocessFailedTasks == null)
            return Optional.empty();
        return Optional.of(this.mustReprocessFailedTasks);
    }

    @Override
    public Optional<Duration> getFailedTaskDetectionRange() {
        if(this.failedTaskDetectionRange == null)
            return Optional.empty();
        return Optional.of(this.failedTaskDetectionRange);
    }

    @Override
    public Optional<Short> getFailedTaskRetryLimit() {
        if(this.failedTaskRetryLimit == null)
            return Optional.empty();
        return Optional.of(this.failedTaskRetryLimit);
    }

    @Override
    public Optional<Boolean> getMustReprocessDeadTasks() {
        if(this.mustReprocessDeadTasks == null)
            return Optional.empty();
        return Optional.of(this.mustReprocessDeadTasks);
    }

    @Override
    public Optional<Duration> getDeadTaskDetectionRange() {
        if(this.deadTaskDetectionRange == null)
            return Optional.empty();
        return Optional.of(this.deadTaskDetectionRange);
    }

    @Override
    public Optional<Short> getDeadTaskRetryLimit() {
        if(this.deadTaskRetryLimit == null)
            return Optional.empty();
        return Optional.of(this.deadTaskRetryLimit);
    }

    @Override
    public Optional<Integer> getMaximumNumberOfBlocksLimit() {
        if(this.maximumNumberOfBlocksLimit == null)
            return Optional.empty();
        return Optional.of(this.maximumNumberOfBlocksLimit);
    }

    public void setMustReprocessFailedTasks(boolean mustReprocessFailedTasks) {
        this.mustReprocessFailedTasks = mustReprocessFailedTasks;
    }

    public void setFailedTaskDetectionRange(Duration failedTaskDetectionRange) {
        this.failedTaskDetectionRange = failedTaskDetectionRange;
    }

    public void setFailedTaskRetryLimit(short failedTaskRetryLimit) {
        this.failedTaskRetryLimit = failedTaskRetryLimit;
    }

    public void setMustReprocessDeadTasks(boolean mustReprocessDeadTasks) {
        this.mustReprocessDeadTasks = mustReprocessDeadTasks;
    }

    public void setDeadTaskDetectionRange(Duration deadTaskDetectionRange) {
        this.deadTaskDetectionRange = deadTaskDetectionRange;
    }

    public void setDeadTaskRetryLimit(short deadTaskRetryLimit) {
        this.deadTaskRetryLimit = deadTaskRetryLimit;
    }

    public void setMaximumNumberOfBlocksLimit(int maximumNumberOfBlocksLimit) {
        this.maximumNumberOfBlocksLimit = maximumNumberOfBlocksLimit;
    }

    public void setTaskDeathMode(TaskDeathMode taskDeathMode) {
        this.taskDeathMode = taskDeathMode;
    }

    public void setBlockType(BlockType blockType) {
        this.blockType = blockType;
    }

    public void setFromDate(Instant fromDate) {
        this.fromDate = fromDate;
    }

    public void setToDate(Instant toDate) {
        this.toDate = toDate;
    }

    public void setMaxBlockTimespan(Duration maxBlockTimespan) {
        this.maxBlockTimespan = maxBlockTimespan;
    }

    public void setFromNumber(Long fromNumber) {
        this.fromNumber = fromNumber;
    }

    public void setToNumber(Long toNumber) {
        this.toNumber = toNumber;
    }

    public void setMaxBlockNumberRange(long maxBlockNumberRange) {
        this.maxBlockNumberRange = maxBlockNumberRange;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public void setMaxBlockSize(short maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
    }

    public void setListUpdateMode(ListUpdateMode listUpdateMode) {
        this.listUpdateMode = listUpdateMode;
    }

    public void setUncommittedItemsThreshold(int uncommittedItemsThreshold) {
        this.uncommittedItemsThreshold = uncommittedItemsThreshold;
    }

    public void setReprocessOption(ReprocessOption reprocessOption) {
        this.reprocessOption = reprocessOption;
    }

    public void setReferenceValueToReprocess(String referenceValueToReprocess) {
        this.referenceValueToReprocess = referenceValueToReprocess;
    }
}
