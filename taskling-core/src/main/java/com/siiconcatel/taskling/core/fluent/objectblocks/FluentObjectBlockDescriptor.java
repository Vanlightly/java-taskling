package com.siiconcatel.taskling.core.fluent.objectblocks;

import com.siiconcatel.taskling.core.fluent.OverrideConfigurationDescriptor;
import com.siiconcatel.taskling.core.fluent.ReprocessScopeDescriptor;

public interface FluentObjectBlockDescriptor<T> {
    /**
     * Instructs the execution context to generate a block context
     * with the given object
     * @param data
     * @return
     */
    OverrideConfigurationDescriptor withObject(T data);

    /**
     * Instructs the execution context to not include any previously
     * processed blocks (that failed or are pending)
     * @return
     */
    OverrideConfigurationDescriptor withNoNewBlocks();

    /**
     * Instructs the execution context to reprocess a block...
     * @return
     */
    ReprocessScopeDescriptor reprocess();
}
