package com.siiconcatel.taskling.core.contexts;

public interface BlockContext {
    /**
     * Changes the status of the block execution to started
     */
    void start();

    /**
     * Changes the status of the block execution to completed
     */
    void complete();

    /**
     * Changes the status of the block execution to failed
     */
    void failed();

    /**
     * Changes the status of the block execution to failed
     * @param message A reason for the failure
     */
    void failed(String message);
}
