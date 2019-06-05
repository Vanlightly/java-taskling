package com.siiconcatel.taskling.core.configuration;

import java.time.Duration;
import java.time.Instant;

public class TaskConfig {
    
    private String applicationName;
    private String taskName;
    private int datastoreTimeoutSeconds;
    private String datastoreConnectionString;

    // concurrency
    private boolean enabled;
    private int concurrencyLimit;

    // clean up
    private int keepListItemsForDays;
    private int keepGeneralDataForDays;
    private int minimumCleanUpIntervalHours;

    // death detection configuration
    private boolean usesKeepAliveMode;
    private double keepAliveIntervalMinutes;
    private double keepAliveDeathThresholdMinutes;
    private double timePeriodDeathThresholdMinutes;

    // reprocess failed taskexecution
    private boolean reprocessFailedTasks;
    private Duration reprocessFailedTasksDetectionRange;
    private int failedTaskRetryLimit;

    // reprocess dead taskexecution
    private boolean reprocessDeadTasks ;
    private Duration reprocessDeadTasksDetectionRange;
    private int deadTaskRetryLimit;

    // Blocks
    private boolean compressData;
    private int maxBlocksToGenerate;
    private int maxLengthForNonCompressedData;
    private int maxStatusReason;

    private Instant dateLoaded;

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

    public int getDatastoreTimeoutSeconds() {
        return datastoreTimeoutSeconds;
    }

    public void setDatastoreTimeoutSeconds(int datastoreTimeoutSeconds) {
        this.datastoreTimeoutSeconds = datastoreTimeoutSeconds;
    }

    public String getDatastoreConnectionString() {
        return datastoreConnectionString;
    }

    public void setDatastoreConnectionString(String datastoreConnectionString) {
        this.datastoreConnectionString = datastoreConnectionString;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public int getKeepListItemsForDays() {
        return keepListItemsForDays;
    }

    public void setKeepListItemsForDays(int keepListItemsForDays) {
        this.keepListItemsForDays = keepListItemsForDays;
    }

    public int getKeepGeneralDataForDays() {
        return keepGeneralDataForDays;
    }

    public void setKeepGeneralDataForDays(int keepGeneralDataForDays) {
        this.keepGeneralDataForDays = keepGeneralDataForDays;
    }

    public int getMinimumCleanUpIntervalHours() {
        return minimumCleanUpIntervalHours;
    }

    public void setMinimumCleanUpIntervalHours(int minimumCleanUpIntervalHours) {
        this.minimumCleanUpIntervalHours = minimumCleanUpIntervalHours;
    }

    public boolean usesKeepAliveMode() {
        return usesKeepAliveMode;
    }

    public void setUsesKeepAliveMode(boolean usesKeepAliveMode) {
        this.usesKeepAliveMode = usesKeepAliveMode;
    }

    public double getKeepAliveIntervalMinutes() {
        return keepAliveIntervalMinutes;
    }

    public void setKeepAliveIntervalMinutes(double keepAliveIntervalMinutes) {
        this.keepAliveIntervalMinutes = keepAliveIntervalMinutes;
    }

    public double getKeepAliveDeathThresholdMinutes() {
        return keepAliveDeathThresholdMinutes;
    }

    public void setKeepAliveDeathThresholdMinutes(double keepAliveDeathThresholdMinutes) {
        this.keepAliveDeathThresholdMinutes = keepAliveDeathThresholdMinutes;
    }

    public double getTimePeriodDeathThresholdMinutes() {
        return timePeriodDeathThresholdMinutes;
    }

    public void setTimePeriodDeathThresholdMinutes(double timePeriodDeathThresholdMinutes) {
        this.timePeriodDeathThresholdMinutes = timePeriodDeathThresholdMinutes;
    }

    public boolean isReprocessFailedTasks() {
        return reprocessFailedTasks;
    }

    public void setReprocessFailedTasks(boolean reprocessFailedTasks) {
        this.reprocessFailedTasks = reprocessFailedTasks;
    }

    public Duration getReprocessFailedTasksDetectionRange() {
        return reprocessFailedTasksDetectionRange;
    }

    public void setReprocessFailedTasksDetectionRange(Duration reprocessFailedTasksDetectionRange) {
        this.reprocessFailedTasksDetectionRange = reprocessFailedTasksDetectionRange;
    }

    public int getFailedTaskRetryLimit() {
        return failedTaskRetryLimit;
    }

    public void setFailedTaskRetryLimit(int failedTaskRetryLimit) {
        this.failedTaskRetryLimit = failedTaskRetryLimit;
    }

    public boolean isReprocessDeadTasks() {
        return reprocessDeadTasks;
    }

    public void setReprocessDeadTasks(boolean reprocessDeadTasks) {
        this.reprocessDeadTasks = reprocessDeadTasks;
    }

    public Duration getReprocessDeadTasksDetectionRange() {
        return reprocessDeadTasksDetectionRange;
    }

    public void setReprocessDeadTasksDetectionRange(Duration reprocessDeadTasksDetectionRange) {
        this.reprocessDeadTasksDetectionRange = reprocessDeadTasksDetectionRange;
    }

    public int getDeadTaskRetryLimit() {
        return deadTaskRetryLimit;
    }

    public void setDeadTaskRetryLimit(int deadTaskRetryLimit) {
        this.deadTaskRetryLimit = deadTaskRetryLimit;
    }

    public int getMaxBlocksToGenerate() {
        return maxBlocksToGenerate;
    }

    public void setMaxBlocksToGenerate(int maxBlocksToGenerate) {
        this.maxBlocksToGenerate = maxBlocksToGenerate;
    }

    public int getMaxLengthForNonCompressedData() {
        return maxLengthForNonCompressedData;
    }

    public void setMaxLengthForNonCompressedData(int maxLengthForNonCompressedData) {
        this.maxLengthForNonCompressedData = maxLengthForNonCompressedData;
    }

    public boolean shouldCompressData() {
        return compressData;
    }

    public void setCompressData(boolean compressData) {
        this.compressData = compressData;
    }

    public int getMaxStatusReason() {
        return maxStatusReason;
    }

    public void setMaxStatusReason(int maxStatusReason) {
        this.maxStatusReason = maxStatusReason;
    }

    public Instant getDateLoaded() {
        return dateLoaded;
    }

    public void setDateLoaded(Instant dateLoaded) {
        this.dateLoaded = dateLoaded;
    }
}
