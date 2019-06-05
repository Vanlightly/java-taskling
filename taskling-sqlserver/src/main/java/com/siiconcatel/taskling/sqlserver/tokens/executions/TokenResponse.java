package com.siiconcatel.taskling.sqlserver.tokens.executions;

import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.GrantStatus;

import java.time.Instant;

public class TokenResponse {
    private String executionTokenId;
    private Instant startedAt;
    private GrantStatus grantStatus;

    public TokenResponse() {}

    public TokenResponse(String executionTokenId, Instant startedAt, GrantStatus grantStatus) {
        this.executionTokenId = executionTokenId;
        this.startedAt = startedAt;
        this.grantStatus = grantStatus;
    }

    public String getExecutionTokenId() {
        return executionTokenId;
    }

    public void setExecutionTokenId(String executionTokenId) {
        this.executionTokenId = executionTokenId;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public GrantStatus getGrantStatus() {
        return grantStatus;
    }

    public void setGrantStatus(GrantStatus grantStatus) {
        this.grantStatus = grantStatus;
    }
}
