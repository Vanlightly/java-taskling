package com.siiconcatel.taskling.core.blocks.factories;

import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlockContextImpl;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlockImpl;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlockResponse;
import com.siiconcatel.taskling.core.blocks.requests.ObjectBlockRequest;
import com.siiconcatel.taskling.core.contexts.ObjectBlockContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.*;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.BlockExecutionCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindBlocksOfTaskRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindDeadBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.FindFailedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.ForcedObjectBlockQueueItem;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common.forcedblocks.QueuedForcedBlocksRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ObjectBlockCreateRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.utils.StringUtils;
import com.siiconcatel.taskling.core.utils.WaitUtils;
import com.siiconcatel.taskling.core.serde.TasklingSerde;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ObjectBlockFactoryImpl extends BlockFactoryBase implements ObjectBlockFactory {

    private final ObjectBlockRepository objectBlockRepository;

    public ObjectBlockFactoryImpl(BlockRepository blockRepository,
                                  TaskExecutionRepository taskExecutionRepository,
                                  ObjectBlockRepository objectBlockRepository)
    {
        super(taskExecutionRepository, blockRepository);
        this.objectBlockRepository = objectBlockRepository;
    }

    public <T> ObjectBlockResponse<T> generateObjectBlocks(ObjectBlockRequest<T> blockRequest)
    {
        List<ObjectBlockContext<T>> blockContexts = new ArrayList<>();
        boolean newObjectIncluded = false;

        if (!StringUtils.isNullOrEmpty(blockRequest.getReprocessReferenceValue()))
        {
            blockContexts = loadObjectBlocksOfTask(blockRequest);
        }
        else
        {
            List<ObjectBlockContext<T>> forceBlocks = getForcedObjectBlocks(blockRequest);
            blockContexts.addAll(forceBlocks);

            if (getBlocksRemaining(blockRequest, blockContexts) > 0)
                loadFailedAndDeadObjectBlocks(blockRequest, blockContexts);

            if (getBlocksRemaining(blockRequest, blockContexts) > 0 && blockRequest.getObjectData() != null)
            {
                ObjectBlockContext<T> newBlock = generateNewObjectBlock(blockRequest);
                blockContexts.add(newBlock);
                newObjectIncluded = true;
            }
        }

        if (blockContexts.isEmpty())
        {
            logEmptyBlockEvent(blockRequest.getTaskExecutionId(),
                    blockRequest.getApplicationName(),
                    blockRequest.getTaskName());
        }

        List<ObjectBlockContext<T>> sorted = blockContexts.stream()
                .sorted(Comparator.comparing(x -> Long.parseLong(x.getBlock().getObjectBlockId())))
                .collect(Collectors.toList());

        return new ObjectBlockResponse<>(sorted, newObjectIncluded);
    }

    public <T> ObjectBlock<T> getLastObjectBlock(Class<T> objectType, LastBlockRequest lastBlockRequest) {
        ProtoObjectBlock block = objectBlockRepository.getLastObjectBlock(lastBlockRequest);
        if(block == null)
            return null;

        return convertToObjectBlock(objectType, block);
    }

    private <T> List<ObjectBlockContext<T>> loadObjectBlocksOfTask(ObjectBlockRequest<T> blockRequest) {
        FindBlocksOfTaskRequest failedBlockRequest = new FindBlocksOfTaskRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                blockRequest.getReprocessReferenceValue(),
                blockRequest.getReprocessOption());

        List<ProtoObjectBlock> protoBlocksOfTask = blockRepository.findObjectBlocksOfTask(failedBlockRequest);

        if (protoBlocksOfTask.isEmpty())
            return new ArrayList<ObjectBlockContext<T>>();

        List<ObjectBlock<T>> blocksOfTask = convertToObjectBlocks(blockRequest.getObjectClass(), protoBlocksOfTask);
        return createObjectBlockContexts(blockRequest, blocksOfTask);
    }

    private <T> List<ObjectBlockContext<T>> getForcedObjectBlocks(ObjectBlockRequest<T> blockRequest) {
        QueuedForcedBlocksRequest forcedBlockRequest = new QueuedForcedBlocksRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType());

        List<ForcedObjectBlockQueueItem> queuedForcedBlocks = blockRepository.getQueuedForcedObjectBlocks(forcedBlockRequest);

        List<ObjectBlockContext<T>> forcedBlocks = new ArrayList<>();
        for (ForcedObjectBlockQueueItem queuedForcedBlock : queuedForcedBlocks) {
            ObjectBlock<T> objBlock = convertToObjectBlock(blockRequest.getObjectClass(), queuedForcedBlock.getObjectBlock());
            forcedBlocks.add(createObjectBlockContext(
                    blockRequest,
                    objBlock,
                    String.valueOf(queuedForcedBlock.getForcedBlockQueueId())));
        }

        if (!forcedBlocks.isEmpty()) {
            dequeueForcedBlocks(
                    blockRequest,
                    forcedBlocks
                            .stream()
                            .map(x -> x.getForcedBlockQueueId())
                            .collect(Collectors.toList()));

        }

        return forcedBlocks;
    }

    private <T> void loadFailedAndDeadObjectBlocks(ObjectBlockRequest<T> blockRequest,
                                                   List<ObjectBlockContext<T>> blocks) {
        int blocksRemaining = getBlocksRemaining(blockRequest, blocks);
        if (blockRequest.isReprocessDeadTasks())
        {
            blocks.addAll(getDeadObjectBlocks(blockRequest, blocksRemaining));
            blocksRemaining = blockRequest.getMaxBlocks() - blocks.size();
        }

        if (getBlocksRemaining(blockRequest, blocks) > 0 && blockRequest.isReprocessFailedTasks())
        {
            blocks.addAll(getFailedObjectBlocks(blockRequest, blocksRemaining));
        }
    }

    private <T> List<ObjectBlockContext<T>> getDeadObjectBlocks(ObjectBlockRequest<T> blockRequest,
                                                                int blockCountLimit) {
        FindDeadBlocksRequest deadBlockRequest = createDeadBlocksRequest(blockRequest, blockCountLimit);
        List<ProtoObjectBlock> deadProtoBlocks = blockRepository.findDeadObjectBlocks(deadBlockRequest);
        List<ObjectBlock<T>> deadBlocks = convertToObjectBlocks(blockRequest.getObjectClass(), deadProtoBlocks);
        return createObjectBlockContexts(blockRequest, deadBlocks);
    }

    private <T> List<ObjectBlockContext<T>> getFailedObjectBlocks(ObjectBlockRequest<T> blockRequest,
                                                                  int blockCountLimit) {
        FindFailedBlocksRequest failedBlockRequest = new FindFailedBlocksRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                Instant.now().minus(blockRequest.getFailedTaskDetectionRange()),
                Instant.now(),
                blockCountLimit,
                blockRequest.getFailedTaskRetryLimit());

        List<ProtoObjectBlock> failedProtoBlocks = blockRepository.findFailedObjectBlocks(failedBlockRequest);
        List<ObjectBlock<T>> failedBlocks = convertToObjectBlocks(blockRequest.getObjectClass(), failedProtoBlocks);
        return createObjectBlockContexts(blockRequest, failedBlocks);
    }

    private <T> List<ObjectBlockContext<T>> createObjectBlockContexts(ObjectBlockRequest<T> blockRequest,
                                                                      List<ObjectBlock<T>> objectBlocks) {
        List<ObjectBlockContext<T>> blocks = new ArrayList<>();
        for(ObjectBlock objectBlock : objectBlocks)
        {
            ObjectBlockContext blockContext = createObjectBlockContext(blockRequest, objectBlock, null);
            blocks.add(blockContext);
        }

        return blocks;
    }

    private <T> ObjectBlockContext<T> createObjectBlockContext(ObjectBlockRequest<T> blockRequest,
                                                               ObjectBlock<T> objectBlock,
                                                               String forcedBlockQueueId) {
        int attempt = objectBlock.getAttempt() + 1;
        BlockExecutionCreateRequest createRequest = new BlockExecutionCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                blockRequest.getBlockType(),
                objectBlock.getObjectBlockId(),
                attempt);

        String blockExecutionId = blockRepository.addObjectBlockExecution(createRequest);
        ObjectBlockContext blockContext = new ObjectBlockContextImpl(objectBlockRepository,
                taskExecutionRepository,
                blockRequest.getApplicationName(),
                blockRequest.getTaskName(),
                blockRequest.getTaskExecutionId(),
                objectBlock,
                blockExecutionId,
                forcedBlockQueueId);

        return blockContext;
    }

    private <T> ObjectBlockContext<T> generateNewObjectBlock(ObjectBlockRequest<T> blockRequest) {
        ObjectBlock<T> newObjectBlock = generateObjectBlock(blockRequest);
        return createObjectBlockContext(blockRequest, newObjectBlock, null);
    }

    private <T> ObjectBlock<T> generateObjectBlock(ObjectBlockRequest<T> blockRequest) {
        String serializedObject = TasklingSerde.serialize(blockRequest.getObjectData(), false);
        // while there is extra cost, it ensures that both serialization and deserialization works before
        // adding the block to the backend data store
        T deserializedObject = TasklingSerde.deserialize(blockRequest.getObjectClass(), serializedObject, false);

        ObjectBlockCreateRequest request = new ObjectBlockCreateRequest(
                new TaskId(blockRequest.getApplicationName(), blockRequest.getTaskName()),
                blockRequest.getTaskExecutionId(),
                serializedObject,
                blockRequest.getCompressionThreshold());

        ProtoObjectBlock objectBlock = blockRepository.addObjectBlock(request).getBlock();
        WaitUtils.waitForMs(10); // guarantee that each block has a unique created date

        return new ObjectBlockImpl<T>(objectBlock.getObjectBlockId(),
                objectBlock.getAttempt(),
                deserializedObject);
    }

    private <T> List<ObjectBlock<T>> convertToObjectBlocks(Class<T> objectClass,
                                                           List<ProtoObjectBlock> protoObjectBlocks) {
        return protoObjectBlocks.stream()
                .map(x -> convertToObjectBlock(objectClass, x))
                .collect(Collectors.toList());
    }

    private <T> ObjectBlock<T> convertToObjectBlock(Class<T> objectClass,
                                                    ProtoObjectBlock protoObjectBlock) {
        T deserializedObj = TasklingSerde.deserialize(objectClass, protoObjectBlock.getObjectData(), true);
        ObjectBlock<T> objectBlock = new ObjectBlockImpl<T>(
                protoObjectBlock.getObjectBlockId(),
                protoObjectBlock.getAttempt(),
                deserializedObj);

        return objectBlock;
    }

    private <T> int getBlocksRemaining(ObjectBlockRequest<T> blockRequest, List<ObjectBlockContext<T>> blocks) {
        return blockRequest.getMaxBlocks() - blocks.size();
    }
}
