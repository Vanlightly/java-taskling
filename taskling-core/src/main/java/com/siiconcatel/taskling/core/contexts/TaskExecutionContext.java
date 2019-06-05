package com.siiconcatel.taskling.core.contexts;

import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
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
import com.siiconcatel.taskling.core.fluent.rangeblocks.FluentDateRangeBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.rangeblocks.FluentNumericRangeBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.listblocks.FluentListBlockDescriptor;
import com.siiconcatel.taskling.core.fluent.listblocks.FluentListBlockWithHeaderDescriptor;
import com.siiconcatel.taskling.core.fluent.objectblocks.FluentObjectBlockDescriptor;
import com.siiconcatel.taskling.core.tasks.TaskExecutionMeta;
import com.siiconcatel.taskling.core.tasks.TaskExecutionMetaWithHeader;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A TaskExecutionContext provides a batch job an API to perform logging, concurrency control,
 * partitioning of work into blocks, tracking and distributed locking (critical sections)
 */
public interface TaskExecutionContext extends AutoCloseable {
    /**
     * Whether or not the context has been started
     * @return Whether or not the context has been started
     */
    boolean isStarted();

    /**
     * Attemps to start the context. If concurrency is limited it may return false.
     * @return Returns true if started correctly and false if concurrency control prevented it
     */
    boolean tryStart();

    /**
     * Attemps to start the context with a string reference value. The reference value can be used to identify the job
     * in the backend store or to perform retries by the reference value.
     * If concurrency is limited it may return false.
     * @param referenceValue The string value to identify the execution context
     * @return Returns true if started correctly and false if concurrency control prevented it
     */
    boolean tryStart(String referenceValue);

    /**
     * Attemps to start the context with some data stored in the context. The data is not persisted to the backend store.
     * If concurrency is limited it may return false.
     * @param executionHeader The data to be stored in the context
     * @return Returns true if started correctly and false if concurrency control prevented it
     */
    <H> boolean tryStart(H executionHeader);

    /**
     * Attemps to start the context with some data stored in the context. The data is not persisted to the backend store.
     * The reference value can be used to identify the job
     * in the backend store or to perform retries by the reference value.
     * If concurrency is limited it may return false.
     * @param executionHeader The data to be stored in the context
     * @param referenceValue The string value to identify the execution context
     * @return Returns true if started correctly and false if concurrency control prevented it
     */
    <H> boolean tryStart(H executionHeader, String referenceValue);

    /**
     * Changes the status to completed
     */
    void complete();

    /**
     * Logs a checkpoint message to the events backend store
     * @param checkpointMessage
     */
    void checkpoint(String checkpointMessage);

    /**
     * Registers an error in the events backend store
     * @param errorMessage The error message
     * @param treatTaskAsFailed True changes the execution status to failed upon completion of the context
     */
    void error(String errorMessage, boolean treatTaskAsFailed);

    /**
     * Retrieves the header data from the context
     * @return The header data
     */
    <H> H getHeader();

    /**
     * Creates a critical section context that allows for the use of a distributed lock
     * @return A CriticalSectionContext
     */
    CriticalSectionContext createCriticalSection();

    /**
     * Returns the last date range block by either created date, start or end date of the block
     * @param lastBlockOrder
     * @return a DateRangeBlock
     */
    DateRangeBlock getLastDateRangeBlock(LastBlockOrder lastBlockOrder);

    /**
     * Returns the last numeric range block by either created date, start or end date of the block
     * @param lastBlockOrder
     * @return a NumericRangeBlock
     */
    NumericRangeBlock getLastNumericRangeBlock(LastBlockOrder lastBlockOrder);

    /**
     * Returns the last list block by created date
     * @param itemType The type of the list block item
     * @return a ListBlock
     */
    <T> ListBlock<T> getLastListBlock(Class<T> itemType);

    /**
     * Returns the last list block with a header by created date
     * @param itemType The type of the list block items
     * @param headerType The type of the header
     * @return A ListBlockWithHeader
     */
    <T,H> ListBlockWithHeader<T,H> getLastListBlockWithHeader(Class<T> itemType, Class<H> headerType);

    /**
     * Returns the last object block by created date
     * @param objectType The type of the object
     * @return An ObjectBlock
     */
    <T> ObjectBlock<T> getLastObjectBlock(Class<T> objectType);

    /**
     * Returns a list of DateRangeBlockContexts based on the fluent API and/or configuration of the task.
     * The returned block contexts may be for new blocks and/or prior blocks that have failed or died.
     * @param fluentBlockRequest A fluent API function
     * @return a list of DateRangeBlockContexts
     */
    DateRangeBlockResponse getDateRangeBlocks(Function<FluentDateRangeBlockDescriptor, Object> fluentBlockRequest);

    /**
     * Returns a list of NumericRangeBlockContexts based on the fluent API and/or configuration of the task
     * The returned block contexts may be for new blocks and/or prior blocks that have failed or died.
     * @param fluentBlockRequest A fluent API function
     * @return a list of NumericRangeBlockContexts
     */
    NumericRangeBlockResponse getNumericRangeBlocks(Function<FluentNumericRangeBlockDescriptor, Object> fluentBlockRequest);

    /**
     * Returns a list of ListBlockContexts based on the fluent API and/or configuration of the task
     * The returned block contexts may be for new blocks and/or prior blocks that have failed or died.
     * @param itemType The type of the list block items
     * @param fluentBlockRequest A fluent API function
     * @return a list of ListBlockContexts
     */
    <T> ListBlockResponse<T> getListBlocks(Class<T> itemType, Function<FluentListBlockDescriptor<T>, Object> fluentBlockRequest);

    /**
     * Returns a list of ListBlockWithHeaderContexts based on the fluent API and/or configuration of the task
     * The returned block contexts may be for new blocks and/or prior blocks that have failed or died.
     * @param itemType The type of the list block items
     * @param headerType The type of the header
     * @param fluentBlockRequest A fluent API function
     * @return a list of ListBlockWithHeaderContexts
     */
    <T,H> ListBlockWithHeaderResponse<T,H> getListBlocksWithHeader(Class<T> itemType,
                                                                   Class<H> headerType,
                                                                   Function<FluentListBlockWithHeaderDescriptor<T,H>, Object> fluentBlockRequest);

    /**
     * Returns a list of ObjectBlockContexts based on the fluent API and/or configuration of the task
     * The returned block contexts may be for new blocks and/or prior blocks that have failed or died.
     * @param fluentBlockRequest A fluent API function
     * @return a list of ObjectBlockContexts
     */
    <T> ObjectBlockResponse<T> getObjectBlocks(Class<T> objectType, Function<FluentObjectBlockDescriptor<T>, Object> fluentBlockRequest);

    /**
     * Returns meta data about the last run task execution context
     * @return a TaskExecutionMeta
     */
    TaskExecutionMeta getLastExecutionMeta();

    /**
     * Returns meta data about the last N task execution contexts
     * @param numberToRetrieve The number of executions to return
     * @return a list of TaskExecutionMeta
     */
    List<TaskExecutionMeta> getLastExecutionMetas(int numberToRetrieve);

    /**
     * Returns meta data about the last run task execution context that has a header
     * @param headerType The type of the header
     * @return a TaskExecutionMetaWithHeader
     */
    <H> TaskExecutionMetaWithHeader<H> getLastExecutionMetaWithHeader(Class<H> headerType);

    /**
     * Returns meta data about the last N task execution contexts with a header
     * @param headerType The type of the header
     * @param numberToRetrieve The number of executions to return
     * @return a list of TaskExecutionMeta
     */
    <H> List<TaskExecutionMetaWithHeader<H>> getLastExecutionMetasWithHeader(Class<H> headerType, int numberToRetrieve);

    /**
     * Calls complete() on the context. Prefer complete() over close() as calls to the backend
     * store are made.
     */
    @Override
    void close();
}
