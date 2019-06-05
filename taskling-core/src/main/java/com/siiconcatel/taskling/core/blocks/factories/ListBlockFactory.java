package com.siiconcatel.taskling.core.blocks.factories;

import com.siiconcatel.taskling.core.blocks.listblocks.ListBlock;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockResponse;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockWithHeader;
import com.siiconcatel.taskling.core.blocks.listblocks.ListBlockWithHeaderResponse;
import com.siiconcatel.taskling.core.blocks.requests.*;
import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.contexts.ListBlockWithHeaderContext;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.LastBlockRequest;

import java.util.List;

public interface ListBlockFactory {
    <T> ListBlockResponse<T> generateListBlocks(ItemOnlyListBlockRequest<T> blockRequest);
    <T,H> ListBlockWithHeaderResponse<T,H> generateListBlocksWithHeader(ItemHeaderListBlockRequest<T,H> blockRequest);

    <T> ListBlock<T> getLastListBlock(ItemOnlyLastBlockRequest<T> lastBlockRequest);
    <T,H> ListBlockWithHeader<T,H> getLastListBlockWithHeader(ItemHeaderLastBlockRequest<T,H> lastBlockRequest);
}
