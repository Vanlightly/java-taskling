package com.siiconcatel.taskling.sqlserver;

import com.siiconcatel.taskling.core.ClientConnectionSettings;
import com.siiconcatel.taskling.core.ConnectionStore;
import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.core.blocks.factories.*;
import com.siiconcatel.taskling.core.cleanup.CleanUpService;
import com.siiconcatel.taskling.core.cleanup.CleanUpServiceImpl;
import com.siiconcatel.taskling.core.configuration.ConfigurationStore;
import com.siiconcatel.taskling.core.configuration.TaskConfig;
import com.siiconcatel.taskling.core.configuration.TasklingConfigReader;
import com.siiconcatel.taskling.core.configuration.TasklingConfiguration;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.core.executioncontext.TaskExecutionContextImpl;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ObjectBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.RangeBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.cleanup.CleanUpRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.CriticalSectionRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.core.tasks.TaskExecutionOptions;
import com.siiconcatel.taskling.sqlserver.blocks.BlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.blocks.ListBlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.blocks.ObjectBlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.blocks.RangeBlockRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.events.EventsRepository;
import com.siiconcatel.taskling.sqlserver.events.EventsRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.taskexecution.CleanUpRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskExecutionRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.CommonTokenRepository;
import com.siiconcatel.taskling.sqlserver.tokens.CommonTokenRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.criticalsections.CriticalSectionRepositoryMsSql;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenRepository;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenRepositoryMsSql;

import java.time.Duration;

public class SqlServerTasklingClient implements TasklingClient {
    private TaskExecutionRepository taskExecutionRepository;
    private CriticalSectionRepository criticalSectionRepository;
    private RangeBlockFactory rangeBlockFactory;
    private ListBlockFactory listBlockFactory;
    private ObjectBlockFactory objectBlockFactory;
    private RangeBlockRepository rangeBlockRepository;
    private ListBlockRepository listBlockRepository;
    private ObjectBlockRepository objectBlockRepository;
    private CleanUpService cleanUpService;
    private TasklingConfiguration configuration;

    public SqlServerTasklingClient(TasklingConfigReader configurationReader)
    {
        TaskRepository taskRepository = new TaskRepositoryMsSql();
        this.configuration = new ConfigurationStore(configurationReader);
        CommonTokenRepository commonTokenRepository = new CommonTokenRepositoryMsSql();
        ExecutionTokenRepository executionTokenRepository = new ExecutionTokenRepositoryMsSql(commonTokenRepository);
        EventsRepository eventsRepository = new EventsRepositoryMsSql();
        BlockRepository blockRepository = new BlockRepositoryMsSql(taskRepository);

        this.taskExecutionRepository = new TaskExecutionRepositoryMsSql(taskRepository, executionTokenRepository, eventsRepository);
        this.criticalSectionRepository = new CriticalSectionRepositoryMsSql(taskRepository, commonTokenRepository);
        this.rangeBlockRepository = new RangeBlockRepositoryMsSql(taskRepository);
        this.listBlockRepository = new ListBlockRepositoryMsSql(taskRepository);
        this.objectBlockRepository = new ObjectBlockRepositoryMsSql(taskRepository);
        this.rangeBlockFactory = new RangeBlockFactoryImpl(blockRepository, taskExecutionRepository, rangeBlockRepository);
        this.listBlockFactory = new ListBlockFactoryImpl(blockRepository, taskExecutionRepository, listBlockRepository);
        this.objectBlockFactory = new ObjectBlockFactoryImpl(blockRepository, taskExecutionRepository, objectBlockRepository);
        CleanUpRepository cleanUpRepository = new CleanUpRepositoryMsSql(taskRepository);
        this.cleanUpService = new CleanUpServiceImpl(configuration, cleanUpRepository, taskExecutionRepository);
    }

    public SqlServerTasklingClient(TasklingConfigReader configurationReader,
                          CustomDependencies customDependencies)
    {
        if (customDependencies.getTaskRepository() == null)
            customDependencies.setTaskRepository(new TaskRepositoryMsSql());

        if (customDependencies.getConfiguration() == null)
            this.configuration = new ConfigurationStore(configurationReader);
        else
            this.configuration = customDependencies.getConfiguration();

        if (customDependencies.getCommonTokenRepository() == null)
            customDependencies.setCommonTokenRepository(new CommonTokenRepositoryMsSql());

        if (customDependencies.getExecutionTokenRepository() == null)
            customDependencies.setExecutionTokenRepository(new ExecutionTokenRepositoryMsSql(customDependencies.getCommonTokenRepository()));

        if (customDependencies.getEventsRepository() == null)
            customDependencies.setEventsRepository(new EventsRepositoryMsSql());

        if (customDependencies.getTaskExecutionRepository() != null)
            this.taskExecutionRepository = customDependencies.getTaskExecutionRepository();
        else
            this.taskExecutionRepository = new TaskExecutionRepositoryMsSql(customDependencies.getTaskRepository(),
                    customDependencies.getExecutionTokenRepository(),
                    customDependencies.getEventsRepository());

        if (customDependencies.getCriticalSectionRepository() != null)
            this.criticalSectionRepository = customDependencies.getCriticalSectionRepository();
        else
            this.criticalSectionRepository = new CriticalSectionRepositoryMsSql(customDependencies.getTaskRepository(),
                    customDependencies.getCommonTokenRepository());

        if (customDependencies.getBlockRepository() == null)
            customDependencies.setBlockRepository(new BlockRepositoryMsSql(customDependencies.getTaskRepository()));

        if (customDependencies.getRangeBlockRepository() != null)
            this.rangeBlockRepository = customDependencies.getRangeBlockRepository();
        else
            this.rangeBlockRepository = new RangeBlockRepositoryMsSql(customDependencies.getTaskRepository());

        if (customDependencies.getListBlockRepository() != null)
            this.listBlockRepository = customDependencies.getListBlockRepository();
        else
            this.listBlockRepository = new ListBlockRepositoryMsSql(customDependencies.getTaskRepository());

        if (customDependencies.getObjectBlockRepository() != null)
            this.objectBlockRepository = customDependencies.getObjectBlockRepository();
        else
            this.objectBlockRepository = new ObjectBlockRepositoryMsSql(customDependencies.getTaskRepository());

        if (customDependencies.getRangeBlockFactory() != null)
            this.rangeBlockFactory = customDependencies.getRangeBlockFactory();
        else
            this.rangeBlockFactory = new RangeBlockFactoryImpl(customDependencies.getBlockRepository(),
                    taskExecutionRepository,
                    rangeBlockRepository);

        if (customDependencies.getListBlockFactory() != null)
            this.listBlockFactory = customDependencies.getListBlockFactory();
        else
            this.listBlockFactory = new ListBlockFactoryImpl(customDependencies.getBlockRepository(),
                    taskExecutionRepository,
                    listBlockRepository);

        if (customDependencies.getObjectBlockFactory() != null)
            this.objectBlockFactory = customDependencies.getObjectBlockFactory();
        else
            this.objectBlockFactory = new ObjectBlockFactoryImpl(customDependencies.getBlockRepository(),
                    taskExecutionRepository,
                    objectBlockRepository);

        if (customDependencies.getCleanUpRepository() == null)
            customDependencies.setCleanUpRepository(new CleanUpRepositoryMsSql(customDependencies.getTaskRepository()));

        if (customDependencies.getCleanUpService() != null)
            this.cleanUpService = customDependencies.getCleanUpService();
        else
            this.cleanUpService = new CleanUpServiceImpl(configuration,
                    customDependencies.getCleanUpRepository(),
                    customDependencies.getTaskExecutionRepository());
    }

    public TaskExecutionContext createTaskExecutionContext(String applicationName,
                                                           String taskName)
    {
        loadConnectionSettings(applicationName, taskName);

        return new TaskExecutionContextImpl(configuration,
                taskExecutionRepository,
                criticalSectionRepository,
                rangeBlockFactory,
                listBlockFactory,
                objectBlockFactory,
                rangeBlockRepository,
                cleanUpService,
                applicationName,
                taskName,
                loadTaskExecutionOptions(applicationName, taskName));
    }

    private void loadConnectionSettings(String applicationName, String taskName)
    {
        TaskConfig taskConfiguration = configuration.getTaskConfiguration(applicationName, taskName);
        ClientConnectionSettings connectionSettings = new ClientConnectionSettings(
                taskConfiguration.getDatastoreConnectionString(),
                Duration.ofSeconds(taskConfiguration.getDatastoreTimeoutSeconds()));

        ConnectionStore.getInstance().setConnection(new TaskId(applicationName, taskName), connectionSettings);
    }

    private TaskExecutionOptions loadTaskExecutionOptions(String applicationName, String taskName)
    {
        TaskConfig taskConfiguration = configuration.getTaskConfiguration(applicationName, taskName);

        TaskExecutionOptions executionOptions = new TaskExecutionOptions();

        if(taskConfiguration.usesKeepAliveMode()) {
            executionOptions.setTaskDeathMode(TaskDeathMode.KeepAlive);
            executionOptions.setDeathThreshold(Duration.ofSeconds((long)(taskConfiguration.getKeepAliveDeathThresholdMinutes() * 60)));
            executionOptions.setKeepAliveInterval(Duration.ofSeconds((long)(taskConfiguration.getKeepAliveIntervalMinutes() * 60)));
        }
        else {
            executionOptions.setTaskDeathMode(TaskDeathMode.Override);
            executionOptions.setDeathThreshold(Duration.ofSeconds((long)(taskConfiguration.getTimePeriodDeathThresholdMinutes() * 60)));
        }
        executionOptions.setConcurrencyLimit(taskConfiguration.getConcurrencyLimit());
        executionOptions.setEnabled(taskConfiguration.isEnabled());

        return executionOptions;
    }
}
