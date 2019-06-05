package com.siiconcatel.taskling.core.fluent;

import java.time.Duration;

public interface FluentBlockSettingsDescriptor {

    /**
     * Instructs the execution context to look for and return previously executed blocks that failed,
     * whose start date falls within the specified time duration.
     * Any blocks that have had more attempts than the retryLimit are ignored
     * @param detectionRange
     * @param retryLimit The limit on retries of a given block
     * @return
     */
    FluentBlockSettingsDescriptor reprocessFailedTasks(Duration detectionRange, short retryLimit);

    /**
     * Instructs the execution context to look for and return previously executed blocks that died,
     * whose start date falls within the specified time duration.
     * Any blocks that have had more attempts than the retryLimit are ignored
     * @param detectionRange
     * @param retryLimit The limit on retries of a given block
     * @return
     */
    FluentBlockSettingsDescriptor reprocessDeadTasks(Duration detectionRange, short retryLimit);

    /**
     * The maximum number of block contexts to generate. Block contexts are
     * generated in the order of precedence:
     * 1. forced
     * 2. failed
     * 3. dead
     * 4. New
     * @param maximumNumberOfBlocks
     * @return
     */
    CompleteDescriptor maximumBlocksToGenerate(int maximumNumberOfBlocks);
}
