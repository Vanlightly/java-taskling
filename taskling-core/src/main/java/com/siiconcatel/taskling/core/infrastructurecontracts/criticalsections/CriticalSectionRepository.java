package com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections;

public interface CriticalSectionRepository {
    StartCriticalSectionResponse start(StartCriticalSectionRequest startRequest);
    CompleteCriticalSectionResponse complete(CompleteCriticalSectionRequest completeCriticalSectionRequest);
}
