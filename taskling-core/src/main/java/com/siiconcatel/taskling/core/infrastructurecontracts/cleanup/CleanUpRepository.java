package com.siiconcatel.taskling.core.infrastructurecontracts.cleanup;

public interface CleanUpRepository {
    boolean cleanOldData(CleanUpRequest cleanUpRequest);
}
