package com.siiconcatel.taskling.core.blocks.factories;

import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlock;
import com.siiconcatel.taskling.core.blocks.objectblocks.ObjectBlockResponse;
import com.siiconcatel.taskling.core.blocks.requests.ObjectBlockRequest;
import com.siiconcatel.taskling.core.contexts.ObjectBlockContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.objectblocks.ProtoObjectBlock;

import java.util.List;

public interface ObjectBlockFactory {
    <T> ObjectBlockResponse<T> generateObjectBlocks(ObjectBlockRequest<T> blockRequest);
    <T> ObjectBlock<T> getLastObjectBlock(Class<T> objectType, LastBlockRequest lastRangeBlockRequest);
}
