package com.siiconcatel.taskling.core.fluent;

public interface ReprocessScopeDescriptor {
    /**
     * Instructs the execution context to reprocess all blocks of...
     * @return
     */
    ReprocessTaskDescriptor allBlocks();

    /**
     * Instructs the execution context to reprocess only failed and pending blocks of...
     * @return
     */
    ReprocessTaskDescriptor pendingAndFailedBlocks();
}
