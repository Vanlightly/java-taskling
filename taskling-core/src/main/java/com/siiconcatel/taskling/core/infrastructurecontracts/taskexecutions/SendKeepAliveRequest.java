package com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions;

import com.siiconcatel.taskling.core.infrastructurecontracts.RequestBase;

public class SendKeepAliveRequest extends RequestBase {
    private String ExecutionTokenId;

    public String getExecutionTokenId() {
        return ExecutionTokenId;
    }

    public void setExecutionTokenId(String executionTokenId) {
        ExecutionTokenId = executionTokenId;
    }
}
