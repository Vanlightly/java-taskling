package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

public class TaskDefinition {
    private int TaskDefinitionId;
    private String ApplicationName;
    private String TaskName;

    public int getTaskDefinitionId() {
        return TaskDefinitionId;
    }

    public void setTaskDefinitionId(int taskDefinitionId) {
        TaskDefinitionId = taskDefinitionId;
    }

    public String getApplicationName() {
        return ApplicationName;
    }

    public void setApplicationName(String applicationName) {
        ApplicationName = applicationName;
    }

    public String getTaskName() {
        return TaskName;
    }

    public void setTaskName(String taskName) {
        TaskName = taskName;
    }
}
