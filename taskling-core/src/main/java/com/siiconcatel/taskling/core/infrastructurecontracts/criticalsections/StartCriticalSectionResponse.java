package com.siiconcatel.taskling.core.infrastructurecontracts.criticalsections;

import com.siiconcatel.taskling.core.infrastructurecontracts.ResponseBase;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.GrantStatus;

public class StartCriticalSectionResponse extends ResponseBase {
    private GrantStatus grantStatus;

    public StartCriticalSectionResponse(GrantStatus grantStatus) {
        this.grantStatus = grantStatus;
    }

    public GrantStatus getGrantStatus() {
        return grantStatus;
    }

    public void setGrantStatus(GrantStatus grantStatus) {
        this.grantStatus = grantStatus;
    }
}
