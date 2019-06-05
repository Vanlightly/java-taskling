package com.siiconcatel.taskling.core.infrastructurecontracts;

import java.util.Objects;

public class TaskId
{
    public TaskId(String applicationName, String taskName)
    {
        this.applicationName = applicationName;
        this.taskName = taskName;
    }

    private String applicationName;
    private String taskName;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskId taskId = (TaskId) o;
        return applicationName.equals(taskId.applicationName) &&
                taskName.equals(taskId.taskName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applicationName, taskName);
    }
}
