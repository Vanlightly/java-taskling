package com.siiconcatel.taskling.core.blocks.factories;

import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlockResponse;
import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlockResponse;
import com.siiconcatel.taskling.core.blocks.requests.DateRangeBlockRequest;
import com.siiconcatel.taskling.core.blocks.requests.NumericRangeBlockRequest;
import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.NumericRangeBlockContext;

import java.util.List;

public interface RangeBlockFactory  {
    DateRangeBlockResponse generateDateRangeBlocks(DateRangeBlockRequest blockRequest);
    NumericRangeBlockResponse generateNumericRangeBlocks(NumericRangeBlockRequest blockRequest);
}
