package com.siiconcatel.taskling.sqlserver;

import com.siiconcatel.taskling.core.blocks.factories.ListBlockFactory;
import com.siiconcatel.taskling.core.blocks.factories.ObjectBlockFactory;
import com.siiconcatel.taskling.core.blocks.factories.RangeBlockFactory;
import com.siiconcatel.taskling.core.cleanup.CleanUpService;
import com.siiconcatel.taskling.core.configuration.TasklingConfiguration;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ObjectBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.RangeBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.cleanup.CleanUpRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.CriticalSectionRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.sqlserver.events.EventsRepository;
import com.siiconcatel.taskling.sqlserver.tokens.CommonTokenRepository;
import com.siiconcatel.taskling.sqlserver.tokens.executions.ExecutionTokenRepository;

public class CustomDependencies {
    private TaskRepository taskRepository;
    private TasklingConfiguration configuration;
    private TaskExecutionRepository taskExecutionRepository;
    private ExecutionTokenRepository executionTokenRepository;
    private CommonTokenRepository commonTokenRepository;
    private EventsRepository eventsRepository;
    private CriticalSectionRepository criticalSectionRepository;
    private RangeBlockFactory rangeBlockFactory;
    private ListBlockFactory listBlockFactory;
    private ObjectBlockFactory objectBlockFactory;
    private BlockRepository blockRepository;
    private RangeBlockRepository rangeBlockRepository;
    private ListBlockRepository listBlockRepository;
    private ObjectBlockRepository objectBlockRepository;
    private CleanUpService cleanUpService;
    private CleanUpRepository cleanUpRepository;

    public CustomDependencies(TaskRepository taskRepository, TasklingConfiguration configuration, TaskExecutionRepository taskExecutionRepository, ExecutionTokenRepository executionTokenRepository, CommonTokenRepository commonTokenRepository, EventsRepository eventsRepository, CriticalSectionRepository criticalSectionRepository, RangeBlockFactory rangeBlockFactory, ListBlockFactory listBlockFactory, ObjectBlockFactory objectBlockFactory, BlockRepository blockRepository, RangeBlockRepository rangeBlockRepository, ListBlockRepository listBlockRepository, ObjectBlockRepository objectBlockRepository, CleanUpService cleanUpService, CleanUpRepository cleanUpRepository) {
        this.taskRepository = taskRepository;
        this.configuration = configuration;
        this.taskExecutionRepository = taskExecutionRepository;
        this.executionTokenRepository = executionTokenRepository;
        this.commonTokenRepository = commonTokenRepository;
        this.eventsRepository = eventsRepository;
        this.criticalSectionRepository = criticalSectionRepository;
        this.rangeBlockFactory = rangeBlockFactory;
        this.listBlockFactory = listBlockFactory;
        this.objectBlockFactory = objectBlockFactory;
        this.blockRepository = blockRepository;
        this.rangeBlockRepository = rangeBlockRepository;
        this.listBlockRepository = listBlockRepository;
        this.objectBlockRepository = objectBlockRepository;
        this.cleanUpService = cleanUpService;
        this.cleanUpRepository = cleanUpRepository;
    }

    public TaskRepository getTaskRepository() {
        return taskRepository;
    }

    public void setTaskRepository(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    public TasklingConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(TasklingConfiguration configuration) {
        this.configuration = configuration;
    }

    public TaskExecutionRepository getTaskExecutionRepository() {
        return taskExecutionRepository;
    }

    public void setTaskExecutionRepository(TaskExecutionRepository taskExecutionRepository) {
        this.taskExecutionRepository = taskExecutionRepository;
    }

    public ExecutionTokenRepository getExecutionTokenRepository() {
        return executionTokenRepository;
    }

    public void setExecutionTokenRepository(ExecutionTokenRepository executionTokenRepository) {
        this.executionTokenRepository = executionTokenRepository;
    }

    public CommonTokenRepository getCommonTokenRepository() {
        return commonTokenRepository;
    }

    public void setCommonTokenRepository(CommonTokenRepository commonTokenRepository) {
        this.commonTokenRepository = commonTokenRepository;
    }

    public EventsRepository getEventsRepository() {
        return eventsRepository;
    }

    public void setEventsRepository(EventsRepository eventsRepository) {
        this.eventsRepository = eventsRepository;
    }

    public CriticalSectionRepository getCriticalSectionRepository() {
        return criticalSectionRepository;
    }

    public void setCriticalSectionRepository(CriticalSectionRepository criticalSectionRepository) {
        this.criticalSectionRepository = criticalSectionRepository;
    }

    public RangeBlockFactory getRangeBlockFactory() {
        return rangeBlockFactory;
    }

    public void setRangeBlockFactory(RangeBlockFactory rangeBlockFactory) {
        this.rangeBlockFactory = rangeBlockFactory;
    }

    public ListBlockFactory getListBlockFactory() {
        return listBlockFactory;
    }

    public void setListBlockFactory(ListBlockFactory listBlockFactory) {
        this.listBlockFactory = listBlockFactory;
    }

    public ObjectBlockFactory getObjectBlockFactory() {
        return objectBlockFactory;
    }

    public void setObjectBlockFactory(ObjectBlockFactory objectBlockFactory) {
        this.objectBlockFactory = objectBlockFactory;
    }

    public BlockRepository getBlockRepository() {
        return blockRepository;
    }

    public void setBlockRepository(BlockRepository blockRepository) {
        this.blockRepository = blockRepository;
    }

    public RangeBlockRepository getRangeBlockRepository() {
        return rangeBlockRepository;
    }

    public void setRangeBlockRepository(RangeBlockRepository rangeBlockRepository) {
        this.rangeBlockRepository = rangeBlockRepository;
    }

    public ListBlockRepository getListBlockRepository() {
        return listBlockRepository;
    }

    public void setListBlockRepository(ListBlockRepository listBlockRepository) {
        this.listBlockRepository = listBlockRepository;
    }

    public ObjectBlockRepository getObjectBlockRepository() {
        return objectBlockRepository;
    }

    public void setObjectBlockRepository(ObjectBlockRepository objectBlockRepository) {
        this.objectBlockRepository = objectBlockRepository;
    }

    public CleanUpService getCleanUpService() {
        return cleanUpService;
    }

    public void setCleanUpService(CleanUpService cleanUpService) {
        this.cleanUpService = cleanUpService;
    }

    public CleanUpRepository getCleanUpRepository() {
        return cleanUpRepository;
    }

    public void setCleanUpRepository(CleanUpRepository cleanUpRepository) {
        this.cleanUpRepository = cleanUpRepository;
    }
}
