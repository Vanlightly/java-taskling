package com.siiconcatel.taskling.core.contexts;

import java.time.Duration;

/**
 * A context that manages a distributed lock
 */
public interface CriticalSectionContext {
    /**
     * Return true if a critical section has succesfully been entered
     * aka a lock has been achieved
     * @return
     */
    boolean isActive();

    /**
     * Attempts to enter a critical section
     * Aka, attempts to acquire a lock
     * @return True when successful
     */
    boolean tryStart();

    /**
     * Attempts to enter a critical section
     * Aka, attempts to acquire a lock
     * @param retryInterval period between retries
     * @param numberOfAttempts The maximum number of attempts to acquire the lock
     * @return True when successful
     */
    boolean tryStart(Duration retryInterval, int numberOfAttempts);

    /**
     * Exits the critical section
      */
    void complete();
}
