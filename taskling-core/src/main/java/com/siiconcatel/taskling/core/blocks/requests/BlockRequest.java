package com.siiconcatel.taskling.core.blocks.requests;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.tasks.ReprocessOption;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;

import java.time.Duration;

public class BlockRequest
{
    private String applicationName;
    private String taskName;
    private String taskExecutionId;
    private int maxBlocks;

    private boolean reprocessFailedTasks;
    private Duration failedTaskDetectionRange;
    private int failedTaskRetryLimit;

    private TaskDeathMode taskDeathMode;
    private boolean reprocessDeadTasks;
    private int deadTaskRetryLimit;
    private Duration overrideDeathThreshold;
    private Duration deadTaskDetectionRange;
    private Duration keepAliveDeathThreshold;

    private BlockType blockType;

    private String reprocessReferenceValue;
    private ReprocessOption reprocessOption;

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskExecutionId() {
        return taskExecutionId;
    }

    public void setTaskExecutionId(String taskExecutionId) {
        this.taskExecutionId = taskExecutionId;
    }

    public int getMaxBlocks() {
        return maxBlocks;
    }

    public void setMaxBlocks(int maxBlocks) {
        this.maxBlocks = maxBlocks;
    }

    public boolean isReprocessFailedTasks() {
        return reprocessFailedTasks;
    }

    public void setReprocessFailedTasks(boolean reprocessFailedTasks) {
        this.reprocessFailedTasks = reprocessFailedTasks;
    }

    public Duration getFailedTaskDetectionRange() {
        return failedTaskDetectionRange;
    }

    public void setFailedTaskDetectionRange(Duration failedTaskDetectionRange) {
        this.failedTaskDetectionRange = failedTaskDetectionRange;
    }

    public int getFailedTaskRetryLimit() {
        return failedTaskRetryLimit;
    }

    public void setFailedTaskRetryLimit(int failedTaskRetryLimit) {
        this.failedTaskRetryLimit = failedTaskRetryLimit;
    }

    public TaskDeathMode getTaskDeathMode() {
        return taskDeathMode;
    }

    public void setTaskDeathMode(TaskDeathMode taskDeathMode) {
        this.taskDeathMode = taskDeathMode;
    }

    public boolean isReprocessDeadTasks() {
        return reprocessDeadTasks;
    }

    public void setReprocessDeadTasks(boolean reprocessDeadTasks) {
        this.reprocessDeadTasks = reprocessDeadTasks;
    }

    public int getDeadTaskRetryLimit() {
        return deadTaskRetryLimit;
    }

    public void setDeadTaskRetryLimit(int deadTaskRetryLimit) {
        this.deadTaskRetryLimit = deadTaskRetryLimit;
    }

    public Duration getOverrideDeathThreshold() {
        return overrideDeathThreshold;
    }

    public void setOverrideDeathThreshold(Duration overrideDeathThreshold) {
        this.overrideDeathThreshold = overrideDeathThreshold;
    }

    public Duration getDeadTaskDetectionRange() {
        return deadTaskDetectionRange;
    }

    public void setDeadTaskDetectionRange(Duration deadTaskDetectionRange) {
        this.deadTaskDetectionRange = deadTaskDetectionRange;
    }

    public Duration getKeepAliveDeathThreshold() {
        return keepAliveDeathThreshold;
    }

    public void setKeepAliveDeathThreshold(Duration keepAliveDeathThreshold) {
        this.keepAliveDeathThreshold = keepAliveDeathThreshold;
    }

    public BlockType getBlockType() {
        return blockType;
    }

    public void setBlockType(BlockType blockType) {
        this.blockType = blockType;
    }

    public String getReprocessReferenceValue() {
        return reprocessReferenceValue;
    }

    public void setReprocessReferenceValue(String reprocessReferenceValue) {
        this.reprocessReferenceValue = reprocessReferenceValue;
    }

    public ReprocessOption getReprocessOption() {
        return reprocessOption;
    }

    public void setReprocessOption(ReprocessOption reprocessOption) {
        this.reprocessOption = reprocessOption;
    }
}