package com.siiconcatel.taskling.core.executioncontext;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.blocks.factories.ListBlockFactory;
import com.siiconcatel.taskling.core.blocks.factories.ObjectBlockFactory;
import com.siiconcatel.taskling.core.blocks.factories.RangeBlockFactory;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlock;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockResponse;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockWithHeader;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockWithHeaderResponse;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlockResponse;
import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlockResponse;
import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlockResponse;
import com.siiconcatel.taskling.core.blocks.requests.*;
import com.siiconcatel.taskling.core.cleanup.CleanUpService;
import com.siiconcatel.taskling.core.configuration.TaskConfig;
import com.siiconcatel.taskling.core.configuration.TasklingConfiguration;
import com.siiconcatel.taskling.core.contexts.*;
import com.siiconcatel.taskling.core.criticalsection.CriticalSectionContextImpl;
import com.siiconcatel.taskling.core.criticalsection.CriticalSectionException;
import com.siiconcatel.taskling.core.fluent.listblocks.FluentListBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.listblocks.FluentListBlockDescriptorImpl;
import com.siiconcatel.taskling.core.fluent.listblocks.FluentListBlockWithHeaderDescriptor;
import com.siiconcatel.taskling.core.fluent.listblocks.FluentListBlockWithHeaderDescriptorImpl;
import com.siiconcatel.taskling.core.fluent.objectblocks.FluentObjectBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.objectblocks.FluentObjectBlockDescriptorImpl;
import com.siiconcatel.taskling.core.fluent.rangeblocks.FluentDateRangeBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.rangeblocks.FluentNumericRangeBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.rangeblocks.FluentRangeBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.settings.BlockSettings;
import com.siiconcatel.taskling.core.fluent.settings.ObjectBlockSettings;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ListBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.ObjectBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.RangeBlockRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.CriticalSectionRepository;
import com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections.CriticalSectionType;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.*;
import com.siiconcatel.taskling.core.tasks.TaskDeathMode;
import com.siiconcatel.taskling.core.tasks.TaskExecutionMeta;
import com.siiconcatel.taskling.core.tasks.TaskExecutionMetaWithHeader;
import com.siiconcatel.taskling.core.tasks.TaskExecutionOptions;
import com.siiconcatel.taskling.core.serde.TasklingSerde;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TaskExecutionContextImpl implements TaskExecutionContext {

    private static String TASKLING_VERSION = "1.0";
    private static String NotActiveMessage = "The TaskExecutionExecution has either not been started, or has already terminated";

    private final TaskExecutionRepository taskExecutionRepository;
    private final RangeBlockRepository rangeBlockRepository;
    private final CriticalSectionRepository criticalSectionRepository;
    private final RangeBlockFactory rangeBlockFactory;
    private final ListBlockFactory listBlockFactory;
    private final ObjectBlockFactory objectBlockFactory;
    private final CleanUpService cleanUpService;

    private TaskExecutionInstance taskExecutionInstance;
    private TaskExecutionOptions taskExecutionOptions;
    private boolean startedCalled;
    private boolean completeCalled;
    private boolean executionHasFailed;
    private KeepAliveDaemon keepAliveDaemon;
    private CriticalSectionContext userCriticalSectionContext;
    private CriticalSectionContext clientCriticalSectionContext;
    private TaskConfig taskConfiguration;
    private Object taskExecutionHeader;

    public TaskExecutionContextImpl(TasklingConfiguration configuration,
                                    TaskExecutionRepository taskExecutionRepository,
                                    CriticalSectionRepository criticalSectionRepository,
                                    RangeBlockFactory rangeBlockFactory,
                                    ListBlockFactory listBlockFactory,
                                    ObjectBlockFactory objecrBlockFactory,
                                    RangeBlockRepository rangeBlockRepository,
                                    CleanUpService cleanUpService,
                                    String applicationName,
                                    String taskName,
                                    TaskExecutionOptions taskExecutionOptions)
    {
        this.taskExecutionRepository = taskExecutionRepository;
        this.criticalSectionRepository = criticalSectionRepository;
        this.rangeBlockFactory = rangeBlockFactory;
        this.listBlockFactory = listBlockFactory;
        this.objectBlockFactory = objecrBlockFactory;
        this.rangeBlockRepository = rangeBlockRepository;
        this.cleanUpService = cleanUpService;

        this.taskExecutionInstance = new TaskExecutionInstance();
        this.taskExecutionInstance.setApplicationName(applicationName);
        this.taskExecutionInstance.setTaskName(taskName);

        this.taskExecutionOptions = taskExecutionOptions;

        this.executionHasFailed = false;

        this.taskConfiguration = configuration.getTaskConfiguration(applicationName, taskName);
    }

    public boolean isActive() {
        return startedCalled && !completeCalled;
    }

    @Override
    public void close() {
        if(isActive()) {
            complete();
        }
    }

    public boolean isStarted()
    {
        return isActive();
    }

    public boolean tryStart()
    {
        return tryStart(null);
    }

    public boolean tryStart(String referenceValue)
    {
        if (!taskExecutionOptions.isEnabled())
            return false;

        if (startedCalled)
            throw new TasklingExecutionException("The execution context has already been started");

        startedCalled = true;

        cleanUpOldData();
        TaskExecutionStartRequest startRequest = createStartRequest(referenceValue);

        try
        {
            TaskExecutionStartResponse response = taskExecutionRepository.start(startRequest);
            taskExecutionInstance.setTaskExecutionId(response.getTaskExecutionId());
            taskExecutionInstance.setExecutionTokenId(response.getExecutionTokenId());

            if (response.getGrantStatus() == GrantStatus.Denied)
            {
                complete();
                return false;
            }

            if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
                startKeepAlive();
        }
        catch (Exception e)
        {
            completeCalled = true;
            throw e;
        }

        return true;
    }

    public <H> boolean tryStart(H executionHeader)
    {
        taskExecutionHeader = executionHeader;
        return tryStart();
    }

    public <H> boolean tryStart(H executionHeader, String referenceValue)
    {
        taskExecutionHeader = executionHeader;
        return tryStart(referenceValue);
    }

    public void complete()
    {
        if (isActive())
        {
            completeCalled = true;

            if (keepAliveDaemon != null)
                keepAliveDaemon.stop();

            TaskExecutionCompleteRequest completeRequest = new TaskExecutionCompleteRequest(
                    new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                    taskExecutionInstance.getTaskExecutionId(),
                    taskExecutionInstance.getExecutionTokenId());
            completeRequest.setFailed(executionHasFailed);


            TaskExecutionCompleteResponse response = taskExecutionRepository.complete(completeRequest);
            taskExecutionInstance.setCompletedAt(response.getCompletedAt());
        }
    }

    public void checkpoint(String checkpointMessage)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        TaskExecutionCheckpointRequest request = new TaskExecutionCheckpointRequest(
            new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
            taskExecutionInstance.getTaskExecutionId(),
            checkpointMessage);
        taskExecutionRepository.checkpoint(request);
    }

    public void error(String errorMessage, boolean treatTaskAsFailed)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        executionHasFailed = treatTaskAsFailed;

        TaskExecutionErrorRequest request = new TaskExecutionErrorRequest(
            new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
            taskExecutionInstance.getTaskExecutionId(),
            errorMessage,
            treatTaskAsFailed);

        taskExecutionRepository.error(request);
    }

    public <H> H getHeader()
    {
        if (taskExecutionHeader != null)
            return (H)taskExecutionHeader;

        return null;
    }

    public CriticalSectionContext createCriticalSection()
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        if (isUserCriticalSectionActive())
            throw new CriticalSectionException("Only one user critical section context can be active at a time for one context. Check that you are not nesting critical sections with the same context.");

        userCriticalSectionContext = new CriticalSectionContextImpl(criticalSectionRepository,
                taskExecutionInstance,
                taskExecutionOptions,
                CriticalSectionType.User);

        return userCriticalSectionContext;
    }

    public DateRangeBlockResponse getDateRangeBlocks(Function<FluentDateRangeBlockDescriptor, Object> fluentBlockRequest)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        Object fluentDescriptor = fluentBlockRequest.apply(new FluentRangeBlockDescriptor());
        BlockSettings settings = (BlockSettings)fluentDescriptor;

        DateRangeBlockRequest request = convertToDateRangeBlockRequest(settings);
        if (shouldProtect(request))
        {
            CriticalSectionContext csContext = createClientCriticalSection();
            try
            {
                boolean csStarted = csContext.tryStart(Duration.ofSeconds(20), 3);
                if (csStarted)
                     return rangeBlockFactory.generateDateRangeBlocks(request);

                throw new CriticalSectionException("Could not start a critical section in the alloted time");
            }
            finally
            {
                csContext.complete();
            }
        }
        else
        {
            return rangeBlockFactory.generateDateRangeBlocks(request);
        }
    }



    public NumericRangeBlockResponse getNumericRangeBlocks(Function<FluentNumericRangeBlockDescriptor, Object> fluentBlockRequest)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        Object fluentDescriptor = fluentBlockRequest.apply(new FluentRangeBlockDescriptor());
        BlockSettings settings = (BlockSettings)fluentDescriptor;

        NumericRangeBlockRequest request = convertToNumericRangeBlockRequest(settings);
        if (shouldProtect(request))
        {
            CriticalSectionContext csContext = createClientCriticalSection();
            try
            {
                boolean csStarted = csContext.tryStart(Duration.ofSeconds(20), 3);
                if (csStarted)
                    return rangeBlockFactory.generateNumericRangeBlocks(request);

                throw new CriticalSectionException("Could not start a critical section in the alloted time");
            }
            finally
            {
                csContext.complete();
            }
        }
        else
        {
            return rangeBlockFactory.generateNumericRangeBlocks(request);
        }
    }

    public <T> ListBlockResponse<T> getListBlocks(Class<T> itemType,
                                                       Function<FluentListBlockDescriptor<T>, Object> fluentBlockRequest)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        Object fluentDescriptor = fluentBlockRequest.apply(new FluentListBlockDescriptorImpl<T>());
        BlockSettings settings = (BlockSettings)fluentDescriptor;

        if (settings.getBlockType() == BlockType.List)
        {
            ItemOnlyListBlockRequest<T> request = convertToItemOnlyListBlockRequest(itemType, settings);
            if (shouldProtect(request))
            {
                CriticalSectionContext csContext = createClientCriticalSection();
                try
                {
                    boolean csStarted = csContext.tryStart(Duration.ofSeconds(20), 3);
                    if (csStarted)
                        return listBlockFactory.generateListBlocks(request);
                    throw new CriticalSectionException("Could not start a critical section in the alloted time");
                }
                finally
                {
                    csContext.complete();
                }
            }
            else
            {
                return listBlockFactory.generateListBlocks(request);
            }
        }

        throw new TasklingExecutionException("BlockType not supported");
    }

    public <T,H> ListBlockWithHeaderResponse<T,H> getListBlocksWithHeader(Class<T> itemType,
                                                                     Class<H> headerType,
                                                                     Function<FluentListBlockWithHeaderDescriptor<T,H>, Object> fluentBlockRequest)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        Object fluentDescriptor = fluentBlockRequest.apply(new FluentListBlockWithHeaderDescriptorImpl<T,H>());
        BlockSettings settings = (BlockSettings)fluentDescriptor;

        if (settings.getBlockType() == BlockType.List)
        {
            ItemHeaderListBlockRequest<T,H> request = convertToItemHeaderListBlockRequest(itemType, headerType, settings);
            if (shouldProtect(request))
            {
                CriticalSectionContext csContext = createClientCriticalSection();
                try
                {
                    boolean csStarted = csContext.tryStart(Duration.ofSeconds(20), 3);
                    if (csStarted)
                        return listBlockFactory.generateListBlocksWithHeader(request);
                    throw new CriticalSectionException("Could not start a critical section in the alloted time");
                }
                finally
                {
                    csContext.complete();
                }
            }
            else
            {
                return listBlockFactory.generateListBlocksWithHeader(request);
            }
        }

        throw new TasklingExecutionException("BlockType not supported");
    }

    public <T> ObjectBlockResponse getObjectBlocks(Class<T> objectType, Function<FluentObjectBlockDescriptor<T>, Object> fluentBlockRequest)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        Object fluentDescriptor = fluentBlockRequest.apply(new FluentObjectBlockDescriptorImpl<T>());
        ObjectBlockSettings<T> settings = (ObjectBlockSettings<T>)fluentDescriptor;

        ObjectBlockRequest<T> request = convertToObjectBlockRequest(objectType, settings);
        if (shouldProtect(request))
        {
            CriticalSectionContext csContext = createClientCriticalSection();
            try
            {
                boolean csStarted = csContext.tryStart(Duration.ofSeconds(20), 3);
                if (csStarted)
                    return objectBlockFactory.generateObjectBlocks(request);

                throw new CriticalSectionException("Could not start a critical section in the alloted time");
            }
            finally
            {
                csContext.complete();
            }
        }
        else
        {
            return objectBlockFactory.generateObjectBlocks(request);
        }
    }

    public DateRangeBlock getLastDateRangeBlock(LastBlockOrder lastBlockOrder)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        LastBlockRequest request = new LastBlockRequest(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                BlockType.DateRange);
        request.setLastBlockOrder(lastBlockOrder);

        return rangeBlockRepository.getLastRangeBlock(request);
    }

    public NumericRangeBlock getLastNumericRangeBlock(LastBlockOrder lastBlockOrder)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        LastBlockRequest request = new LastBlockRequest(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                BlockType.NumericRange);
        request.setLastBlockOrder(lastBlockOrder);

        return rangeBlockRepository.getLastRangeBlock(request);
    }

    public <T> ListBlock<T> getLastListBlock(Class<T> itemType)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        ItemOnlyLastBlockRequest<T> request = new ItemOnlyLastBlockRequest<T>(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                BlockType.List,
                itemType);

        return listBlockFactory.getLastListBlock(request);
    }

    public <T,H> ListBlockWithHeader<T,H> getLastListBlockWithHeader(Class<T> itemType, Class<H> headerType)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        ItemHeaderLastBlockRequest<T,H> request = new ItemHeaderLastBlockRequest<T,H>(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                BlockType.List,
                itemType,
                headerType);

        return listBlockFactory.getLastListBlockWithHeader(request);
    }

    public <T> ObjectBlock<T> getLastObjectBlock(Class<T> objectType)
    {
        if (!isActive())
            throw new TasklingExecutionException(NotActiveMessage);

        LastBlockRequest request = new LastBlockRequest(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                BlockType.Object);


        return objectBlockFactory.getLastObjectBlock(objectType, request);
    }

    public TaskExecutionMeta getLastExecutionMeta()
    {
        List<TaskExecutionMeta> metas = getLastExecutionMetas(1);
        if(!metas.isEmpty())
            return metas.get(0);
        else
            return null;
    }

    public List<TaskExecutionMeta> getLastExecutionMetas(int numberToRetrieve)
    {
        TaskExecutionMetaRequest request = createTaskExecutionMetaRequest(numberToRetrieve);

        TaskExecutionMetaResponse response = taskExecutionRepository.getLastExecutionMetas(request);
        if (response.getExecutions() != null && !response.getExecutions().isEmpty())
        {
            return response.getExecutions()
                    .stream()
                    .map(x -> new TaskExecutionMeta(
                            x.getStartedAt(),
                            x.getCompletedAt().orElse(null),
                            x.getStatus(),
                            x.getReferenceValue()))
                    .collect(Collectors.toList());
        }

        return new ArrayList<>();
    }

    public <H> TaskExecutionMetaWithHeader<H> getLastExecutionMetaWithHeader(Class<H> headerType)
    {
        TaskExecutionMetaRequest request = createTaskExecutionMetaRequest(1);

        TaskExecutionMetaResponse response = taskExecutionRepository.getLastExecutionMetas(request);
        if (response.getExecutions() != null && !response.getExecutions().isEmpty())
        {
            TaskExecutionMetaItem meta = response.getExecutions().get(0);
            return new TaskExecutionMetaWithHeader<H>(meta.getStartedAt(),
                    meta.getCompletedAt().orElse(null),
                    meta.getStatus(),
                    meta.getReferenceValue(),
                    TasklingSerde.deserialize(headerType, meta.getHeader(), true));
        }

        return null;
    }

    public <H> List<TaskExecutionMetaWithHeader<H>> getLastExecutionMetasWithHeader(Class<H> headerType, int numberToRetrieve)
    {
        TaskExecutionMetaRequest request = createTaskExecutionMetaRequest(numberToRetrieve);

        TaskExecutionMetaResponse response = taskExecutionRepository.getLastExecutionMetas(request);
        if (response.getExecutions() != null && !response.getExecutions().isEmpty())
        {
            return response.getExecutions()
                    .stream()
                    .map(x -> new TaskExecutionMetaWithHeader<H>(x.getStartedAt(),
                                    x.getCompletedAt().orElse(null),
                                    x.getStatus(),
                                    x.getReferenceValue(),
                                    TasklingSerde.deserialize(headerType, x.getHeader(), true)))
                    .collect(Collectors.toList());
        }

        return new ArrayList<TaskExecutionMetaWithHeader<H>>();
    }


    private void cleanUpOldData()
    {
        cleanUpService.cleanOldData(taskExecutionInstance.getApplicationName(),
                taskExecutionInstance.getTaskName(),
                taskExecutionInstance.getTaskExecutionId());
    }

    private TaskExecutionStartRequest createStartRequest(String referenceValue)
    {
        TaskExecutionStartRequest startRequest = new TaskExecutionStartRequest(
                new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
                taskExecutionOptions.getTaskDeathMode(),
                taskExecutionOptions.getConcurrencyLimit(),
                taskConfiguration.getFailedTaskRetryLimit(),
                taskConfiguration.getDeadTaskRetryLimit());

        setStartRequestValues(startRequest, referenceValue);
        setStartRequestTasklingVersion(startRequest);
        serializeHeaderIfExists(startRequest);

        return startRequest;
    }

    private void setStartRequestTasklingVersion(TaskExecutionStartRequest startRequest)
    {
        startRequest.setTasklingVersion(TASKLING_VERSION);
    }

    private void setStartRequestValues(TaskExecutionStartRequest startRequest, String referenceValue)
    {
        if (taskExecutionOptions.getDeathThreshold() == null)
            throw new TasklingExecutionException("Task death threshold must be set");

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
        {
            if (!taskExecutionOptions.getKeepAliveInterval().isPresent())
                throw new TasklingExecutionException("KeepAliveInterval must be set when using KeepAlive mode");

            startRequest.setKeepAliveDeathThreshold(taskExecutionOptions.getDeathThreshold());
            startRequest.setKeepAliveInterval(taskExecutionOptions.getKeepAliveInterval().get());
        }
        else if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.Override)
        {
            startRequest.setOverrideThreshold(taskExecutionOptions.getDeathThreshold());
        }

        startRequest.setReferenceValue(referenceValue);
    }

    private void serializeHeaderIfExists(TaskExecutionStartRequest startRequest)
    {
        if (taskExecutionHeader != null)
            startRequest.setTaskExecutionHeader(TasklingSerde.serialize(taskExecutionHeader, false));
    }

    private void startKeepAlive()
    {
        SendKeepAliveRequest keepAliveRequest = new SendKeepAliveRequest();
        keepAliveRequest.setTaskId(new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()));
        keepAliveRequest.setTaskExecutionId(taskExecutionInstance.getTaskExecutionId());
        keepAliveRequest.setExecutionTokenId(taskExecutionInstance.getExecutionTokenId());

        keepAliveDaemon = new KeepAliveDaemon(taskExecutionRepository,
                new WeakReference(this),
                keepAliveRequest,
                taskExecutionOptions.getKeepAliveInterval().get());
        keepAliveDaemon.sendKeepAlives();
    }

    private DateRangeBlockRequest convertToDateRangeBlockRequest(BlockSettings settings)
    {
        DateRangeBlockRequest request = new DateRangeBlockRequest();
        request.setApplicationName(taskExecutionInstance.getApplicationName());
        request.setTaskName(taskExecutionInstance.getTaskName());
        request.setTaskExecutionId(taskExecutionInstance.getTaskExecutionId());
        request.setTaskDeathMode(taskExecutionOptions.getTaskDeathMode());

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
            request.setKeepAliveDeathThreshold(taskExecutionOptions.getDeathThreshold());
        else
            request.setOverrideDeathThreshold(taskExecutionOptions.getDeathThreshold());

        if(settings.getFromDate().isPresent())
            request.setRangeBegin(settings.getFromDate().get());

        if(settings.getToDate().isPresent())
            request.setRangeEnd(settings.getToDate().get());

        if(settings.getMaxBlockTimespan().isPresent())
            request.setMaxBlockRange(settings.getMaxBlockTimespan().get());
        request.setReprocessReferenceValue(settings.getReferenceValueToReprocess());
        request.setReprocessOption(settings.getReprocessOption());

        setConfigurationOverridableSettings(request, settings);

        return request;
    }

    private NumericRangeBlockRequest convertToNumericRangeBlockRequest(BlockSettings settings)
    {
        NumericRangeBlockRequest request = new NumericRangeBlockRequest();
        request.setApplicationName(taskExecutionInstance.getApplicationName());
        request.setTaskName(taskExecutionInstance.getTaskName());
        request.setTaskExecutionId(taskExecutionInstance.getTaskExecutionId());
        request.setTaskDeathMode(taskExecutionOptions.getTaskDeathMode());

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
            request.setKeepAliveDeathThreshold(taskExecutionOptions.getDeathThreshold());
        else
            request.setOverrideDeathThreshold(taskExecutionOptions.getDeathThreshold());

        if(settings.getFromNumber().isPresent())
            request.setRangeBegin(settings.getFromNumber().get());

        if(settings.getToNumber().isPresent())
            request.setRangeEnd(settings.getToNumber().get());

        if(settings.getMaxBlockNumberRange().isPresent())
            request.setBlockSize(settings.getMaxBlockNumberRange().get());
        request.setReprocessReferenceValue(settings.getReferenceValueToReprocess());
        request.setReprocessOption(settings.getReprocessOption());

        setConfigurationOverridableSettings(request, settings);

        return request;
    }

    private <T> ItemOnlyListBlockRequest<T> convertToItemOnlyListBlockRequest(Class<T> itemType, BlockSettings settings)
    {
        ItemOnlyListBlockRequest<T> request = new ItemOnlyListBlockRequest<T>(itemType);
        request.setApplicationName(taskExecutionInstance.getApplicationName());
        request.setTaskName(taskExecutionInstance.getTaskName());
        request.setTaskExecutionId(taskExecutionInstance.getTaskExecutionId());
        request.setTaskDeathMode(taskExecutionOptions.getTaskDeathMode());

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
            request.setKeepAliveDeathThreshold(taskExecutionOptions.getDeathThreshold());
        else
            request.setOverrideDeathThreshold(taskExecutionOptions.getDeathThreshold());

        request.setSerializedValues(settings.getValues());
        request.setSerializedHeader(settings.getHeader());
        request.setCompressionThreshold(taskConfiguration.getMaxLengthForNonCompressedData());
        request.setMaxStatusReasonLength(taskConfiguration.getMaxStatusReason());

        request.setMaxBlockSize(settings.getMaxBlockSize());
        request.setListUpdateMode(settings.getListUpdateMode());
        request.setUncommittedItemsThreshold(settings.getUncommittedItemsThreshold());

        request.setReprocessReferenceValue(settings.getReferenceValueToReprocess());
        request.setReprocessOption(settings.getReprocessOption());

        setConfigurationOverridableSettings(request, settings);


        return request;
    }

    private <T,H> ItemHeaderListBlockRequest<T,H> convertToItemHeaderListBlockRequest(Class<T> itemType,
                                                                                      Class<H> headerType,
                                                                                      BlockSettings settings)
    {
        ItemHeaderListBlockRequest<T,H> request = new ItemHeaderListBlockRequest<T,H>(itemType, headerType);
        request.setApplicationName(taskExecutionInstance.getApplicationName());
        request.setTaskName(taskExecutionInstance.getTaskName());
        request.setTaskExecutionId(taskExecutionInstance.getTaskExecutionId());
        request.setTaskDeathMode(taskExecutionOptions.getTaskDeathMode());

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
            request.setKeepAliveDeathThreshold(taskExecutionOptions.getDeathThreshold());
        else
            request.setOverrideDeathThreshold(taskExecutionOptions.getDeathThreshold());

        request.setSerializedValues(settings.getValues());
        request.setSerializedHeader(settings.getHeader());
        request.setCompressionThreshold(taskConfiguration.getMaxLengthForNonCompressedData());
        request.setMaxStatusReasonLength(taskConfiguration.getMaxStatusReason());

        request.setMaxBlockSize(settings.getMaxBlockSize());
        request.setListUpdateMode(settings.getListUpdateMode());
        request.setUncommittedItemsThreshold(settings.getUncommittedItemsThreshold());

        request.setReprocessReferenceValue(settings.getReferenceValueToReprocess());
        request.setReprocessOption(settings.getReprocessOption());

        setConfigurationOverridableSettings(request, settings);


        return request;
    }

    private <T> ObjectBlockRequest<T> convertToObjectBlockRequest(Class<T> objectType, ObjectBlockSettings<T> settings)
    {
        ObjectBlockRequest request = new ObjectBlockRequest<T>(settings.getObject(), objectType);
        request.setCompressData(taskConfiguration.shouldCompressData());
        request.setCompressionThreshold(taskConfiguration.getMaxLengthForNonCompressedData());
        request.setApplicationName(taskExecutionInstance.getApplicationName());
        request.setTaskName(taskExecutionInstance.getTaskName());
        request.setTaskExecutionId(taskExecutionInstance.getTaskExecutionId());
        request.setTaskDeathMode(taskExecutionOptions.getTaskDeathMode());

        if (taskExecutionOptions.getTaskDeathMode() == TaskDeathMode.KeepAlive)
            request.setKeepAliveDeathThreshold(taskExecutionOptions.getDeathThreshold());
        else
            request.setOverrideDeathThreshold(taskExecutionOptions.getDeathThreshold());

        request.setReprocessReferenceValue(settings.getReferenceValueToReprocess());
        request.setReprocessOption(settings.getReprocessOption());

        setConfigurationOverridableSettings(request, settings);

        return request;
    }

    private void setConfigurationOverridableSettings(BlockRequest request, BlockSettings settings)
    {
        if (settings.getMustReprocessDeadTasks().isPresent())
            request.setReprocessDeadTasks(settings.getMustReprocessDeadTasks().get());
        else
            request.setReprocessDeadTasks(taskConfiguration.isReprocessDeadTasks());

        if (settings.getMustReprocessFailedTasks().isPresent())
            request.setReprocessFailedTasks(settings.getMustReprocessFailedTasks().get());
        else
            request.setReprocessFailedTasks(taskConfiguration.isReprocessFailedTasks());

        if (settings.getDeadTaskRetryLimit().isPresent())
            request.setDeadTaskRetryLimit(settings.getDeadTaskRetryLimit().get());
        else
            request.setDeadTaskRetryLimit(taskConfiguration.getDeadTaskRetryLimit());

        if (settings.getFailedTaskRetryLimit().isPresent())
            request.setFailedTaskRetryLimit(settings.getFailedTaskRetryLimit().get());
        else
            request.setFailedTaskRetryLimit(taskConfiguration.getFailedTaskRetryLimit());

        if (request.isReprocessDeadTasks())
        {
            if (settings.getDeadTaskDetectionRange().isPresent())
                request.setDeadTaskDetectionRange(settings.getDeadTaskDetectionRange().get());
            else
                request.setDeadTaskDetectionRange(taskConfiguration.getReprocessDeadTasksDetectionRange());
        }

        if (request.isReprocessFailedTasks())
        {
            if (settings.getFailedTaskDetectionRange().isPresent())
                request.setFailedTaskDetectionRange(settings.getFailedTaskDetectionRange().get());
            else
                request.setFailedTaskDetectionRange(taskConfiguration.getReprocessFailedTasksDetectionRange());
        }

        if (settings.getMaximumNumberOfBlocksLimit().isPresent())
            request.setMaxBlocks(settings.getMaximumNumberOfBlocksLimit().get());
        else
            request.setMaxBlocks(taskConfiguration.getMaxBlocksToGenerate());
    }

    private boolean isUserCriticalSectionActive()
    {
        return userCriticalSectionContext != null && userCriticalSectionContext.isActive();
    }

    private boolean shouldProtect(BlockRequest blockRequest)
    {
        return (blockRequest.isReprocessDeadTasks() || blockRequest.isReprocessFailedTasks()) && !isUserCriticalSectionActive();
    }

    private CriticalSectionContext createClientCriticalSection()
    {
        if (isClientCriticalSectionActive())
            throw new CriticalSectionException("Only one client critical section context can be active at a time");

        this.clientCriticalSectionContext = new CriticalSectionContextImpl(criticalSectionRepository,
                taskExecutionInstance,
                taskExecutionOptions,
                CriticalSectionType.Client);

        return this.clientCriticalSectionContext;
    }

    private boolean isClientCriticalSectionActive()
    {
        return clientCriticalSectionContext != null && clientCriticalSectionContext.isActive();
    }

    private TaskExecutionMetaRequest createTaskExecutionMetaRequest(int numberToRetrieve)
    {
        TaskExecutionMetaRequest request = new TaskExecutionMetaRequest(
            new TaskId(taskExecutionInstance.getApplicationName(), taskExecutionInstance.getTaskName()),
            numberToRetrieve);

        return request;
    }
}
