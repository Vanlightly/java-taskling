package com.siiconcatel.taskling.core.tasks;

import java.time.Duration;
import java.util.Optional;

public class TaskExecutionOptions {
    private TaskDeathMode taskDeathMode;
    private Duration deathThreshold;
    private Duration keepAliveInterval;
    private int concurrencyLimit;
    private boolean enabled;

    public TaskDeathMode getTaskDeathMode() {
        return taskDeathMode;
    }

    public void setTaskDeathMode(TaskDeathMode taskDeathMode) {
        this.taskDeathMode = taskDeathMode;
    }

    public Duration getDeathThreshold() {
        return deathThreshold;
    }

    public void setDeathThreshold(Duration deathThreshold) {
        this.deathThreshold = deathThreshold;
    }

    public Optional<Duration> getKeepAliveInterval() {
        if(keepAliveInterval == null)
            return Optional.empty();

        return Optional.of(keepAliveInterval);
    }

    public void setKeepAliveInterval(Duration keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public int getConcurrencyLimit() {
        return concurrencyLimit;
    }

    public void setConcurrencyLimit(int concurrencyLimit) {
        this.concurrencyLimit = concurrencyLimit;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}