package com.siiconcatel.taskling.core.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.siiconcatel.taskling.core.utils.StringUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConfigurationStore implements TasklingConfiguration {

    private final TasklingConfigReader configurationReader;
    private static HashMap<String, TaskConfig> taskConfigurations;
    private static Lock cacheLock = new ReentrantLock();

    public ConfigurationStore(TasklingConfigReader configurationReader)
    {
        this.configurationReader = configurationReader;
        taskConfigurations = new HashMap<>();
    }

    public TaskConfig getTaskConfiguration(String applicationName, String taskName)
    {
        if (StringUtils.isNullOrEmpty(applicationName))
            throw new TaskConfigurationException("Cannot load a TaskConfig, ApplicationName is null or empty");

        if (StringUtils.isNullOrEmpty(taskName))
            throw new TaskConfigurationException("Cannot load a TaskConfig, TaskName is null or empty");

        cacheLock.lock();
        try
        {
            String key = getCacheKey(applicationName, taskName);
            boolean loadFromConfigFile = false;
            if (!taskConfigurations.containsKey(key))
                loadFromConfigFile = true;
            else if (Duration.between(Instant.now(), taskConfigurations.get(key).getDateLoaded()).getSeconds() > 60)
                loadFromConfigFile = true;

            if (loadFromConfigFile)
            {
                TaskConfig configuration = loadConfiguration(applicationName, taskName);
                configuration.setApplicationName(applicationName);
                configuration.setTaskName(taskName);
                configuration.setDateLoaded(Instant.now());

                taskConfigurations.put(key, configuration);
            }

            return taskConfigurations.get(key);
        }
        finally {
            cacheLock.unlock();
        }
    }

    private String getCacheKey(String applicationName, String taskName)
    {
        return applicationName + "::" + taskName;
    }

    private TaskConfig loadConfiguration(String applicationName, String taskName)
    {
        String configString = getConfigString(applicationName, taskName);
        TaskConfig taskConfiguration = parseConfigString(configString, applicationName, taskName);

        return taskConfiguration;
    }

    private TaskConfig parseConfigString(String configString, String applicationName, String taskName)
    {
        ObjectMapper mapper = new ObjectMapper();

        JsonNode jsonConfig = null;
        try {
            jsonConfig = mapper.readTree(configString);
        }
        catch(IOException e) {
            throw new TaskConfigurationException("Failed reading JSON string", e);
        }

        TaskConfig taskConfiguration = new TaskConfig();
        taskConfiguration.setDatastoreConnectionString(getRequiredStringValue(jsonConfig, "datastoreConnection"));
        taskConfiguration.setConcurrencyLimit(getIntValue(jsonConfig, "concurrency", -1));
        taskConfiguration.setDatastoreTimeoutSeconds(getIntValue(jsonConfig, "datastoreTimeoutSeconds", 120));
        taskConfiguration.setEnabled(getBooleanValue(jsonConfig, "enabled", true));
        taskConfiguration.setKeepGeneralDataForDays(getIntValue(jsonConfig, "generalRetentionDays", 40));
        taskConfiguration.setKeepListItemsForDays(getIntValue(jsonConfig, "listItemRetentionDays", 14));
        taskConfiguration.setMinimumCleanUpIntervalHours(getIntValue(jsonConfig, "minimumCleanUpIntervalMinutes", 1));

        boolean useKeepAlives = getBooleanValue(jsonConfig, "useKeepAliveMode", true);
        taskConfiguration.setUsesKeepAliveMode(useKeepAlives);
        if(useKeepAlives) {
            taskConfiguration.setKeepAliveDeathThresholdMinutes(getRequiredIntValue(jsonConfig, "taskDeathThresholdMinutes"));
            taskConfiguration.setKeepAliveIntervalMinutes(getIntValue(jsonConfig, "keepAliveIntervalMinutes", 1));
        }
        else {
            taskConfiguration.setTimePeriodDeathThresholdMinutes(getRequiredIntValue(jsonConfig, "taskDeathThresholdMinutes"));
        }

        boolean reprocessFailedTasks = getBooleanValue(jsonConfig, "reprocessFailedTasks", false);
        taskConfiguration.setReprocessFailedTasks(reprocessFailedTasks);
        if(reprocessFailedTasks) {
            taskConfiguration.setReprocessFailedTasksDetectionRange(Duration.ofMinutes(getRequiredIntValue(jsonConfig, "reprocessFailedTasksDetectionRangeMinutes")));
            taskConfiguration.setFailedTaskRetryLimit(getRequiredIntValue(jsonConfig, "reprocessFailedTaskLimit"));
        }

        boolean reprocessDeadTasks = getBooleanValue(jsonConfig, "reprocessDeadTasks", false);
        taskConfiguration.setReprocessDeadTasks(reprocessDeadTasks);
        if(reprocessDeadTasks) {
            taskConfiguration.setReprocessDeadTasksDetectionRange(Duration.ofMinutes(getRequiredIntValue(jsonConfig, "reprocessDeadTasksDetectionRangeMinutes")));
            taskConfiguration.setDeadTaskRetryLimit(getRequiredIntValue(jsonConfig, "reprocessDeadTaskLimit"));
        }

        taskConfiguration.setMaxBlocksToGenerate(getIntValue(jsonConfig, "maxBlocksToGenerate", 10000));

        boolean compressData = getBooleanValue(jsonConfig, "compressBlockData", false);
        taskConfiguration.setCompressData(compressData);

        taskConfiguration.setMaxLengthForNonCompressedData(getIntValue(jsonConfig, "compressWhenLongerThan", 1000));
        taskConfiguration.setMaxStatusReason(getIntValue(jsonConfig, "itemStatusReasonMaxLength", 1000000));

        return taskConfiguration;
    }

    private String getRequiredStringValue(JsonNode jsonConfig, String field) {
        if(jsonConfig.has(field))
            return jsonConfig.at("/"+field).asText();

        throw new TaskConfigurationException("Required configuration field " + field + " is missing");
    }

    private String getStringValue(JsonNode jsonConfig, String field, String defaultValue) {
        return jsonConfig.at("/"+field).asText(defaultValue);
    }

    private int getIntValue(JsonNode jsonConfig, String field, int defaultValue) {
        return jsonConfig.at("/"+field).asInt(defaultValue);
    }

    private int getRequiredIntValue(JsonNode jsonConfig, String field) {
        if(jsonConfig.has(field))
            return jsonConfig.at("/"+field).asInt();
        throw new TaskConfigurationException("Required configuration field " + field + " is missing");
    }

    private boolean getBooleanValue(JsonNode jsonConfig, String field, boolean defaultValue) {
        return jsonConfig.at("/"+field).asBoolean(defaultValue);
    }

    private double getDoubleValue(JsonNode jsonConfig, String field, double defaultValue) {
        return jsonConfig.at("/"+field).asDouble(defaultValue);
    }

    private String getConfigString(String applicationName, String taskName)
    {
        return configurationReader.getTaskConfiguration(applicationName, taskName);
    }
}
